use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use log::{info, warn};
use mkt_signal::common::binance_account_mode::{init_binance_account_mode, BinanceAccountMode};
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::funding_rate::ArbMode;
use mkt_signal::pre_trade::auto_collection_service::AutoCollectionService;
use mkt_signal::pre_trade::auto_repay_service::AutoRepayService;
use mkt_signal::pre_trade::intra_bwd_symbol_list::IntraBwdSymbolList;
use mkt_signal::pre_trade::monitor_channel::MonitorChannel;
use mkt_signal::pre_trade::params_load::PreTradeParamsLoader;
use mkt_signal::pre_trade::persist_channel::PersistChannel;
use mkt_signal::pre_trade::resample_channel::ResampleChannel;
use mkt_signal::pre_trade::signal_channel::{
    SignalChannel, DEFAULT_BACKWARD_CHANNEL, DEFAULT_SIGNAL_CHANNEL,
};
use mkt_signal::pre_trade::PreTrade;
use mkt_signal::pre_trade::QueryEngHub;
use mkt_signal::pre_trade::TradeEngHub;
use mkt_signal::signal::common::TradingVenue;
use mkt_signal::strategy::StrategyManager;
use mkt_signal::trade_engine::config::RestConstants;
use mkt_signal::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "pre_trade")]
#[command(about = "Pre-trade risk management and order execution")]
struct Args {
    /// Venue for opening leg (e.g., binance-margin).
    /// If omitted (and hedge_venue also omitted), venues will be inferred from current directory name.
    #[arg(long, value_enum)]
    open_venue: Option<TradingVenue>,

    /// Venue for hedging leg (e.g., binance-futures).
    /// If omitted (and open_venue also omitted), venues will be inferred from current directory name.
    #[arg(long, value_enum)]
    hedge_venue: Option<TradingVenue>,
}

fn infer_venues_from_cwd() -> Option<(TradingVenue, TradingVenue)> {
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd.file_name()?.to_string_lossy().to_string();
    let normalized = leaf.to_lowercase().replace('_', "-");

    fn normalize_exchange(ex: &str) -> &str {
        match ex {
            "okx" => "okex",
            _ => ex,
        }
    }

    fn futures_venue(ex: &str) -> Option<TradingVenue> {
        match ex {
            "binance" => Some(TradingVenue::BinanceFutures),
            "okex" => Some(TradingVenue::OkexFutures),
            "gate" => Some(TradingVenue::GateFutures),
            "bybit" => Some(TradingVenue::BybitFutures),
            "bitget" => Some(TradingVenue::BitgetFutures),
            _ => None,
        }
    }

    fn margin_venue(ex: &str) -> Option<TradingVenue> {
        match ex {
            "binance" => Some(TradingVenue::BinanceMargin),
            "okex" => Some(TradingVenue::OkexMargin),
            "gate" => Some(TradingVenue::GateMargin),
            "bybit" => Some(TradingVenue::BybitMargin),
            "bitget" => Some(TradingVenue::BitgetMargin),
            _ => None,
        }
    }

    let parts: Vec<&str> = normalized.split('-').filter(|s| !s.is_empty()).collect();

    // intra: <exchange>-intra-<trade|test|...> → margin × futures (same exchange)
    if parts.len() >= 2 && parts[1] == "intra" {
        let ex = normalize_exchange(parts[0]);
        return Some((margin_venue(ex)?, futures_venue(ex)?));
    }

    // cross: <open>-<hedge>-cross-<trade|test|...> → futures × futures (different exchanges)
    if parts.len() >= 3 && parts[2] == "cross" {
        let open_ex = normalize_exchange(parts[0]);
        let hedge_ex = normalize_exchange(parts[1]);
        return Some((futures_venue(open_ex)?, futures_venue(hedge_ex)?));
    }

    // fr: <exchange>-fr-<trade|test|...> → margin × futures (cross-exchange funding-rate arb)
    if parts.len() >= 2 && parts[1] == "fr" {
        let ex = normalize_exchange(parts[0]);
        return Some((margin_venue(ex)?, futures_venue(ex)?));
    }

    None
}

fn infer_dir_prefix_from_cwd() -> Option<String> {
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd.file_name()?.to_string_lossy().trim().to_string();
    if leaf.is_empty() {
        return None;
    }
    Some(leaf.to_lowercase())
}

fn is_mm_pre_trade_mode(open_venue: TradingVenue, hedge_venue: TradingVenue) -> bool {
    open_venue == hedge_venue
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    // 解析命令行参数
    let args = Args::parse();
    let (open_venue, hedge_venue) = match (args.open_venue, args.hedge_venue) {
        (Some(open), Some(hedge)) => (open, hedge),
        (None, None) => {
            let cwd = std::env::current_dir().ok();
            let inferred = infer_venues_from_cwd().ok_or_else(|| {
                anyhow::anyhow!(
                    "missing --open-venue/--hedge-venue and failed to infer from cwd={:?}; please pass both flags explicitly",
                    cwd
                )
            })?;
            info!(
                "venues inferred from cwd={:?}: open_venue={:?} hedge_venue={:?}",
                cwd, inferred.0, inferred.1
            );
            inferred
        }
        _ => {
            return Err(anyhow::anyhow!(
                "invalid args: --open-venue and --hedge-venue must be provided together, or both omitted"
            ));
        }
    };
    info!(
        "pre_trade starting, open_venue={:?}, hedge_venue={:?}",
        open_venue, hedge_venue
    );
    let need_binance = open_venue.trade_engine_exchange() == "binance"
        || hedge_venue.trade_engine_exchange() == "binance";
    let binance_account_mode = if need_binance {
        Some(init_binance_account_mode("pre_trade"))
    } else {
        None
    };
    let mut required_env: Vec<&str> = Vec::new();
    if open_venue.trade_engine_exchange() == "binance"
        || hedge_venue.trade_engine_exchange() == "binance"
    {
        required_env.extend(["BINANCE_API_KEY", "BINANCE_API_SECRET"]);
    }
    if need_binance {
        required_env.push("BINANCE_ACCOUNT_MODE");
    }
    if open_venue.trade_engine_exchange() == "okex" || hedge_venue.trade_engine_exchange() == "okex"
    {
        required_env.extend(["OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"]);
    }
    if !required_env.is_empty() {
        info!("Required env vars: {}", required_env.join(", "));
    }
    if let Some(mode) = binance_account_mode {
        info!("BINANCE_ACCOUNT_MODE={}", mode.as_str());
    }
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 1. 初始化 PreTradeParamsLoader（从 Redis 加载风控参数）
            info!("Initializing PreTradeParamsLoader singleton...");

            // 使用默认 Redis 设置（127.0.0.1:6379/0）
            // Redis 风控参数按目录 + open/hedge 实例隔离：
            // <dir>:<open>:<hedge>:pre_trade_risk_params
            let mut redis_settings = RedisSettings::default();
            // 统一标准：使用 kebab-case venue slug（例如 okex-margin），与 scripts/ 运维保持一致。
            let dir_prefix = infer_dir_prefix_from_cwd();
            let prefix = match dir_prefix.as_deref() {
                Some(name) if !name.is_empty() => format!(
                    "{}:{}:{}:",
                    name,
                    open_venue.data_pub_slug(),
                    hedge_venue.data_pub_slug()
                ),
                _ => format!(
                    "{}:{}:",
                    open_venue.data_pub_slug(),
                    hedge_venue.data_pub_slug()
                ),
            };
            redis_settings.prefix = Some(prefix.clone());
            info!(
                "pre_trade redis key prefix={:?} (dir_prefix={:?})",
                redis_settings.prefix.as_deref(),
                dir_prefix
            );

            let loader = PreTradeParamsLoader::instance();
            loader
                .load_from_redis(&redis_settings, dir_prefix.as_deref(), open_venue, hedge_venue)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to load pre-trade risk params from Redis (open={:?} hedge={:?}): {err:#}. expected hash key: '{}'",
                        open_venue,
                        hedge_venue,
                        format!("{}pre_trade_risk_params", prefix),
                    )
                });
            info!("Risk parameters loaded successfully");

            // 打印风控参数三线表
            loader.print_params_table();

            // 启动后台刷新任务（60s 间隔）
            PreTradeParamsLoader::start_background_refresh(
                redis_settings,
                dir_prefix.clone(),
                open_venue,
                hedge_venue,
            );
            info!("Background refresh task started (interval: 60s)");

            // IntraArb 部署：拉取 trade_signal 维护的 intra_bwd_trade_symbols 作为
            // PM 借贷白名单（仅在 UNIFIED 账户下生效，详见 open_strategy_common）。
            // 启动时同步加载一次，避免首个开仓信号到来时白名单还是空的；
            // 之后由后台任务按 60s 周期 reload。
            // 与 trade_signal 共用同一份 Redis key（无 prefix）以保证两侧视图一致。
            if ArbMode::from_venues(open_venue, hedge_venue) == ArbMode::IntraArb {
                let bwd_key_suffix = open_venue.trade_engine_exchange().to_string();
                let bwd_redis = RedisSettings::default();
                if let Err(err) =
                    IntraBwdSymbolList::load_from_redis(&bwd_redis, &bwd_key_suffix).await
                {
                    warn!(
                        "intra_bwd 借贷白名单初次加载失败 key_suffix='{}': {:#}",
                        bwd_key_suffix, err
                    );
                }
                IntraBwdSymbolList::start_background_refresh(bwd_redis, bwd_key_suffix);
            }

            info!(
                "MM-only pre_trade path enabled={} (open={:?} hedge={:?})",
                is_mm_pre_trade_mode(open_venue, hedge_venue),
                open_venue,
                hedge_venue
            );

            // 2. 初始化 StrategyManager
            info!("Initializing StrategyManager...");
            let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));

            // 3. 初始化 MonitorChannel（包含所有账户管理器）
            info!("Initializing MonitorChannel singleton...");
            if let Err(err) =
                MonitorChannel::init_singleton(
                    strategy_mgr.clone(),
                    open_venue,
                    hedge_venue,
                    binance_account_mode,
                )
                .await
            {
                return Err(err);
            }
            info!("MonitorChannel initialized successfully");

            // 3.1 启动 Binance 自动还币任务（每小时 55 分触发）
            if matches!(open_venue, TradingVenue::BinanceMargin)
                || matches!(hedge_venue, TradingVenue::BinanceMargin)
            {
                info!("auto repay enabled (binance-margin detected)");
                let binance_api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
                let binance_api_secret = std::env::var("BINANCE_API_SECRET").unwrap_or_default();
                if binance_api_key.trim().is_empty() || binance_api_secret.trim().is_empty() {
                    warn!("BINANCE_API_KEY/SECRET missing; auto repay task will still start but API calls may fail");
                }
                let rest_base = match std::env::var("BINANCE_PAPI_URL")
                    .or_else(|_| std::env::var("BINANCE_FAPI_URL"))
                {
                    Ok(url) if !url.trim().is_empty() => url,
                    _ => RestConstants::BINANCE_BASE_URL.to_string(),
                };
                AutoRepayService::new(
                    rest_base,
                    binance_api_key,
                    binance_api_secret,
                    RestConstants::RECV_WINDOW_MS,
                )
                .start_auto_repay_task();
            }

            // 3.2 启动 Binance PM 自动资金归集任务：
            // - pre_trade 重启后立即执行一次；
            // - 每天 UTC+8 12:00 执行一次；
            if matches!(open_venue, TradingVenue::BinanceMargin)
                || matches!(hedge_venue, TradingVenue::BinanceMargin)
            {
                let is_unified = matches!(binance_account_mode, Some(BinanceAccountMode::Unified));
                if is_unified {
                    let binance_api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
                    let binance_api_secret = std::env::var("BINANCE_API_SECRET").unwrap_or_default();
                    if binance_api_key.trim().is_empty() || binance_api_secret.trim().is_empty() {
                        warn!(
                            "BINANCE_API_KEY/SECRET missing; auto collection disabled (binance-margin detected)"
                        );
                    } else {
                        let rest_base = match std::env::var("BINANCE_SAPI_URL")
                            .or_else(|_| std::env::var("BINANCE_API_URL"))
                            .or_else(|_| std::env::var("BINANCE_PAPI_URL"))
                            .or_else(|_| std::env::var("BINANCE_FAPI_URL"))
                        {
                            Ok(url) if !url.trim().is_empty() => url,
                            _ => "https://api.binance.com".to_string(),
                        };

                        info!(
                            "auto collection enabled (binance-margin detected, account_mode={:?}, rest_base={})",
                            binance_account_mode, rest_base
                        );
                        AutoCollectionService::new(
                            rest_base,
                            binance_api_key,
                            binance_api_secret,
                            RestConstants::RECV_WINDOW_MS,
                        )
                        .start_startup_and_daily_task();
                    }
                } else {
                    info!(
                        "auto collection disabled: account_mode={:?} (requires UNIFIED)",
                        binance_account_mode
                    );
                }
            }

            // 4. 初始化 SignalChannel
            info!("Initializing SignalChannel singleton...");
            if let Err(err) =
                SignalChannel::initialize(DEFAULT_SIGNAL_CHANNEL, Some(DEFAULT_BACKWARD_CHANNEL))
            {
                warn!("Failed to initialize SignalChannel: {err:#}");
            } else {
                info!(
                    "SignalChannel initialized on channel: {}",
                    DEFAULT_SIGNAL_CHANNEL
                );
            }

            // 5. 初始化 ResampleChannel
            info!("Initializing ResampleChannel singleton...");
            let exposure_ch = "pre_trade_exposure".to_string();
            let risk_ch = "pre_trade_risk".to_string();
            if let Err(err) = ResampleChannel::initialize(&exposure_ch, &risk_ch) {
                warn!("Failed to initialize ResampleChannel: {err:#}");
            } else {
                info!(
                    "ResampleChannel initialized successfully (exposure={} risk={})",
                    exposure_ch, risk_ch
                );
            }

            ResampleChannel::start_exposure_table_printer(Duration::from_secs(10));

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
                    let binance_account_mode = binance_account_mode;
                    tokio::task::spawn_local(async move {
                    // 定时快照查询：balance 与 position 都要 query。
                    // 目的：
                    // - futures-only 场景也需要 balance（特别是 USDT）用于风控/可用资金判断
                    // - margin-only 场景也需要 position（部分交易所/模式下持仓会通过不同通道补齐）
                    // - 对 OKX，实时仓位数量来自 account stream 的 balance_and_position；
                    //   positions snapshot 主要承担初始化/校准，以及补齐 UPL
                    let need_binance = open_venue.trade_engine_exchange() == "binance"
                        || hedge_venue.trade_engine_exchange() == "binance";
                    let need_okex = open_venue.trade_engine_exchange() == "okex"
                        || hedge_venue.trade_engine_exchange() == "okex";
                    let need_gate = open_venue.trade_engine_exchange() == "gate"
                        || hedge_venue.trade_engine_exchange() == "gate";
                    let need_bybit = open_venue.trade_engine_exchange() == "bybit"
                        || hedge_venue.trade_engine_exchange() == "bybit";
                    let need_bitget = open_venue.trade_engine_exchange() == "bitget"
                        || hedge_venue.trade_engine_exchange() == "bitget";

                    let need_binance_balance = need_binance;
                    let need_binance_um = need_binance;
                    let need_okex_balance = need_okex;
                    let need_okex_swap_positions = need_okex;
                    let need_gate_balance = need_gate;
                    let need_gate_positions = need_gate;
                    let need_bybit_balance = need_bybit;
                    let need_bybit_positions = need_bybit;
                    let need_bitget_balance = need_bitget;
                    let need_bitget_positions = need_bitget;

                    if !need_binance_balance
                        && !need_binance_um
                        && !need_okex_balance
                        && !need_okex_swap_positions
                        && !need_gate_balance
                        && !need_gate_positions
                        && !need_bybit_balance
                        && !need_bybit_positions
                        && !need_bitget_balance
                        && !need_bitget_positions
                    {
                        info!(
                            "snapshot query skipped: venues are {:?}/{:?}; relying on account stream",
                            open_venue, hedge_venue
                        );
                        return;
                    }

                    let mut interval = tokio::time::interval(Duration::from_secs(60));

                    let binance_is_standard =
                        matches!(binance_account_mode, Some(BinanceAccountMode::Standard));
                    let send_snapshot_queries = || {
                        if need_binance_balance {
                            let now = get_timestamp_us();
                            if binance_is_standard {
                                let spot_req = GenericQueryRequest::create(
                                    QueryRequestType::BinanceSpotAccountSnapshotStd,
                                    now,
                                    now,
                                    Bytes::new(),
                                );
                                let _ = QueryEngHub::publish_query_request(
                                    "binance",
                                    &spot_req.to_bytes(),
                                );
                                info!("snapshot query sent: binance spot account snapshot (standard)");

                                let um_req = GenericQueryRequest::create(
                                    QueryRequestType::BinanceUmBalanceSnapshotStd,
                                    now,
                                    now,
                                    Bytes::new(),
                                );
                                let _ = QueryEngHub::publish_query_request(
                                    "binance",
                                    &um_req.to_bytes(),
                                );
                                info!("snapshot query sent: binance UM balance snapshot (standard)");
                            } else {
                                let req = GenericQueryRequest::create(
                                    QueryRequestType::BinancePmBalanceSnapshot,
                                    now,
                                    now,
                                    Bytes::new(),
                                );
                                let _ = QueryEngHub::publish_query_request(
                                    "binance",
                                    &req.to_bytes(),
                                );
                                info!("snapshot query sent: binance PM balance snapshot");
                            }
                        }
                        if need_binance_um {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                if binance_is_standard {
                                    QueryRequestType::BinanceUmAccountSnapshotStd
                                } else {
                                    QueryRequestType::BinanceUmAccountSnapshot
                                },
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("binance", &req.to_bytes());
                            if binance_is_standard {
                                info!("snapshot query sent: binance UM account snapshot (standard)");
                            } else {
                                info!("snapshot query sent: binance UM account snapshot");
                            }
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
                        if need_gate_balance {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::GateUnifiedBalanceSnapshot,
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("gate", &req.to_bytes());
                            info!("snapshot query sent: gate unified balance snapshot");
                        }
                        if need_gate_positions {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::GateUnifiedPositionsSnapshot,
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("gate", &req.to_bytes());
                            info!("snapshot query sent: gate futures positions snapshot (includes upl)");
                        }
                        if need_bybit_balance {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::BybitAccountBalanceSnapshot,
                                now,
                                now,
                                Bytes::from_static(b"accountType=UNIFIED"),
                            );
                            let _ = QueryEngHub::publish_query_request("bybit", &req.to_bytes());
                            info!("snapshot query sent: bybit unified wallet balance snapshot");
                        }
                        if need_bybit_positions {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::BybitPositionsSnapshot,
                                now,
                                now,
                                Bytes::from_static(b"category=linear&settleCoin=USDT"),
                            );
                            let _ = QueryEngHub::publish_query_request("bybit", &req.to_bytes());
                            info!("snapshot query sent: bybit linear positions snapshot");
                        }
                        if need_bitget_balance {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::BitgetAccountBalanceSnapshot,
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("bitget", &req.to_bytes());
                            info!("snapshot query sent: bitget unified account balance snapshot");
                        }
                        if need_bitget_positions {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::BitgetPositionsSnapshot,
                                now,
                                now,
                                Bytes::from_static(b"category=USDT-FUTURES"),
                            );
                            let _ = QueryEngHub::publish_query_request("bitget", &req.to_bytes());
                            info!("snapshot query sent: bitget UTA current positions snapshot");
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
