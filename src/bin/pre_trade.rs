use account_common::bybit_auth::BybitCredentials;
use account_common::gate_auth::GateCredentials;
use account_common::{init_binance_account_mode, BinanceAccountMode};
use anyhow::Result;
use clap::Parser;
use log::{info, warn};
use mkt_signal::pre_trade::auto_collection_service::AutoCollectionService;
use mkt_signal::pre_trade::auto_repay::{BinanceRepayer, BybitRepayer, GateRepayer};
use mkt_signal::pre_trade::auto_repay_service::AutoRepayService;
use mkt_signal::pre_trade::binance_fr_position_limit_guard::BinanceFrPositionLimitGuard;
use mkt_signal::pre_trade::bitget_position_tier_guard::BitgetPositionTierGuard;
use mkt_signal::pre_trade::gate_fr_risk_limit_guard::GateFrRiskLimitGuard;
use mkt_signal::pre_trade::intra_bwd_symbol_list::IntraBwdSymbolList;
use mkt_signal::pre_trade::intra_unimmr_open_lock::IntraUnimmrOpenLock;
use mkt_signal::pre_trade::leverage_guard::LeverageGuard;
use mkt_signal::pre_trade::monitor_channel::MonitorChannel;
use mkt_signal::pre_trade::params_load::PreTradeParamsLoader;
use mkt_signal::pre_trade::persist_channel::PersistChannel;
use mkt_signal::pre_trade::publish_snapshot_queries;
use mkt_signal::pre_trade::rebalance_usdt::{RebalanceUsdtConfig, RebalanceUsdtService};
use mkt_signal::pre_trade::resample_channel::ResampleChannel;
use mkt_signal::pre_trade::runtime_flags::enable_ipc_fast_poll;
use mkt_signal::pre_trade::signal_channel::{
    SignalChannel, DEFAULT_BACKWARD_CHANNEL, DEFAULT_SIGNAL_CHANNEL,
};
use mkt_signal::pre_trade::taker_decision_model::PreTradeTakerDecisionModel;
use mkt_signal::pre_trade::QueryEngHub;
use mkt_signal::pre_trade::TradeEngHub;
use mkt_signal::pre_trade::{
    IntraBwdRefreshConfig, ParamRefreshConfig, PreTrade, SnapshotQueryConfig,
    TakerDecisionModelRefreshConfig,
};
use mkt_signal::strategy::StrategyManager;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;
use runtime_common::redis_client::RedisSettings;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use trade_engine::config::RestConstants;
use trade_signal::ArbMode;

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

    /// 绑定到指定 CPU 核（可选）；未提供则尝试 PRE_TRADE_CORE 环境变量
    #[arg(long)]
    core: Option<usize>,
}

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

fn normalized_dir_parts(dir_name: &str) -> Vec<String> {
    dir_name
        .to_lowercase()
        .replace('_', "-")
        .split('-')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn infer_venues_from_dir_name(dir_name: &str) -> Option<(TradingVenue, TradingVenue)> {
    let parts = normalized_dir_parts(dir_name);

    // intra: <exchange>-intra-<trade|test|...> → margin × futures (same exchange)
    if parts.len() >= 2 && parts[1] == "intra" {
        let ex = normalize_exchange(&parts[0]);
        return Some((margin_venue(ex)?, futures_venue(ex)?));
    }

    // cross: <open>-<hedge>-cross-<trade|test|...> → futures × futures (different exchanges)
    if parts.len() >= 3 && parts[2] == "cross" {
        let open_ex = normalize_exchange(&parts[0]);
        let hedge_ex = normalize_exchange(&parts[1]);
        return Some((futures_venue(open_ex)?, futures_venue(hedge_ex)?));
    }

    // fr: <exchange>-fr-<trade|test|...> → margin × futures (cross-exchange funding-rate arb)
    if parts.len() >= 2 && parts[1] == "fr" {
        let ex = normalize_exchange(&parts[0]);
        return Some((margin_venue(ex)?, futures_venue(ex)?));
    }

    None
}

fn infer_venues_from_cwd() -> Option<(TradingVenue, TradingVenue)> {
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd.file_name()?.to_string_lossy().to_string();
    infer_venues_from_dir_name(&leaf)
}

fn infer_arb_mode_from_dir_name(dir_name: &str) -> Option<ArbMode> {
    let parts = normalized_dir_parts(dir_name);
    if parts.len() >= 2 && parts[1] == "intra" {
        return Some(ArbMode::IntraArb);
    }
    if parts.len() >= 3 && parts[2] == "cross" {
        return Some(ArbMode::CrossArb);
    }
    if parts.len() >= 2 && parts[1] == "fr" {
        return Some(ArbMode::FundingArb);
    }
    None
}

fn infer_arb_mode_from_cwd() -> Option<ArbMode> {
    let cwd = std::env::current_dir().ok()?;
    let leaf = cwd.file_name()?.to_string_lossy().to_string();
    infer_arb_mode_from_dir_name(&leaf)
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
    maybe_pin_current_thread(args.core, "PRE_TRADE_CORE")?;
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
    let venue_arb_mode = ArbMode::from_venues(open_venue, hedge_venue);
    let cwd_arb_mode = infer_arb_mode_from_cwd();
    let arb_mode = cwd_arb_mode.unwrap_or(venue_arb_mode);
    info!(
        "pre_trade starting, open_venue={:?}, hedge_venue={:?}, arb_mode={} (venue_derived={})",
        open_venue,
        hedge_venue,
        arb_mode.as_str(),
        venue_arb_mode.as_str()
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
            let fast_poll = enable_ipc_fast_poll();
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

            let param_refresh = ParamRefreshConfig::new(
                redis_settings.clone(),
                dir_prefix.clone(),
                open_venue,
                hedge_venue,
            );
            info!("Risk parameter refresh configured (interval: 60s)");

            // IntraArb 部署：拉取 trade_signal 维护的 intra_bwd_trade_symbols 作为
            // PM 借贷白名单（仅在 UNIFIED 账户下生效，详见 open_strategy_common）。
            // 启动时同步加载一次，避免首个开仓信号到来时白名单还是空的；
            // 之后由后台任务按 60s 周期 reload。
            // 与 trade_signal 共用同一份 Redis key（无 prefix）以保证两侧视图一致。
            let mut intra_bwd_refresh = None;
            let mut taker_decision_model_refresh = None;
            if arb_mode == ArbMode::IntraArb {
                let bwd_key_suffix = open_venue.trade_engine_exchange().to_string();
                let bwd_env_name = dir_prefix.clone().unwrap_or_else(|| {
                    panic!("intra_bwd_trade_symbols requires an env directory prefix")
                });
                let bwd_redis = RedisSettings::default();
                if let Err(err) =
                    IntraBwdSymbolList::load_from_redis(&bwd_redis, &bwd_env_name, &bwd_key_suffix).await
                {
                    warn!(
                        "intra_bwd 借贷白名单初次加载失败 key_suffix='{}': {:#}",
                        bwd_key_suffix, err
                    );
                }
                if fast_poll {
                    intra_bwd_refresh = Some(IntraBwdRefreshConfig::new(bwd_redis, bwd_env_name, bwd_key_suffix));
                } else {
                    IntraBwdSymbolList::start_background_refresh(bwd_redis, bwd_env_name, bwd_key_suffix);
                }
            }

            if matches!(arb_mode, ArbMode::IntraArb | ArbMode::CrossArb) {
                let strategy_redis = RedisSettings::default();
                match PreTradeTakerDecisionModel::reload_config_global(
                    &strategy_redis,
                    dir_prefix.as_deref(),
                    open_venue,
                    hedge_venue,
                )
                .await
                {
                    Ok(Some(stats)) => info!(
                        "pre_trade taker decision model initial load key={} fields={} ready_symbols={} cached_symbols={} invalid_payloads={}",
                        stats.output_hash_key,
                        stats.fields,
                        stats.ready_symbols,
                        stats.cached_symbols,
                        stats.invalid_payloads
                    ),
                    Ok(None) => info!("pre_trade taker decision model initial load: disabled"),
                    Err(err) => warn!(
                        "pre_trade taker decision model initial load failed; fallback to no model: {:#}",
                        err
                    ),
                }

                taker_decision_model_refresh = Some(TakerDecisionModelRefreshConfig::new(
                    strategy_redis,
                    dir_prefix.clone(),
                    open_venue,
                    hedge_venue,
                ));
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
                    arb_mode,
                    binance_account_mode,
                )
                .await
            {
                return Err(err);
            }
            info!("MonitorChannel initialized successfully");
            IntraUnimmrOpenLock::initialize(arb_mode, binance_account_mode);

            // 3.1 启动多交易所自动还款服务（启动即跑一次 + 每小时 :55 UTC）。
            //     - Binance：仅 PM (UNIFIED) 账户模式注册，端点 /papi/v1/repayLoan
            //     - Gate   ：UNIFIED 账户，端点 POST /api/v4/unified/loans (type=repay)
            //     - Bybit  ：UNIFIED 账户，端点 POST /v5/account/quick-repayment（占位实现，
            //               已知近期返回 "no liability" 但 borrowAmount 不归零，待端点确认）
            let mut auto_repay_service = None;
            {
                let mut repay_svc = AutoRepayService::new();

                let binance_in_play = matches!(open_venue, TradingVenue::BinanceMargin)
                    || matches!(hedge_venue, TradingVenue::BinanceMargin);
                let binance_is_unified =
                    matches!(binance_account_mode, Some(BinanceAccountMode::Unified));
                if binance_in_play && binance_is_unified {
                    let binance_api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
                    let binance_api_secret =
                        std::env::var("BINANCE_API_SECRET").unwrap_or_default();
                    if binance_api_key.trim().is_empty() || binance_api_secret.trim().is_empty() {
                        warn!(
                            "binance auto-repay disabled: BINANCE_API_KEY/SECRET missing"
                        );
                    } else {
                        let rest_base = match std::env::var("BINANCE_PAPI_URL")
                            .or_else(|_| std::env::var("BINANCE_FAPI_URL"))
                        {
                            Ok(url) if !url.trim().is_empty() => url,
                            _ => RestConstants::BINANCE_BASE_URL.to_string(),
                        };
                        repay_svc.register(Box::new(BinanceRepayer::new(
                            rest_base,
                            binance_api_key,
                            binance_api_secret,
                            RestConstants::RECV_WINDOW_MS,
                        )));
                    }
                } else if binance_in_play {
                    info!(
                        "binance auto-repay disabled: account_mode={:?} (requires UNIFIED/PM)",
                        binance_account_mode
                    );
                }

                if matches!(open_venue, TradingVenue::GateMargin)
                    || matches!(hedge_venue, TradingVenue::GateMargin)
                {
                    match GateCredentials::from_env() {
                        Ok(creds) => repay_svc.register(Box::new(GateRepayer::new(creds))),
                        Err(e) => {
                            warn!("gate auto-repay disabled: {e}");
                        }
                    }
                }

                if matches!(open_venue, TradingVenue::BybitMargin)
                    || matches!(hedge_venue, TradingVenue::BybitMargin)
                {
                    match BybitCredentials::from_env() {
                        Ok(creds) => repay_svc.register(Box::new(BybitRepayer::new(creds))),
                        Err(e) => {
                            warn!("bybit auto-repay disabled: {e}");
                        }
                    }
                }

                if !repay_svc.is_empty() {
                    auto_repay_service = Some(repay_svc);
                }
            }

            // 3.2 启动 Binance PM 自动资金归集任务：
            // - pre_trade 重启后立即执行一次；
            // - 每天 UTC+8 12:00 执行一次；
            let mut auto_collection_service = None;
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
                        auto_collection_service = Some(AutoCollectionService::new(
                            rest_base,
                            binance_api_key,
                            binance_api_secret,
                            RestConstants::RECV_WINDOW_MS,
                        ));
                    }
                } else {
                    info!(
                        "auto collection disabled: account_mode={:?} (requires UNIFIED)",
                        binance_account_mode
                    );
                }
            }

            // 4. 启动前杠杆保护：读取当前 online symbols，并阻塞完成 5/4/3 杠杆设置。
            //    运行期只做内存同步检查；未知 symbol 会触发全局 ArbOpen 阻断。
            info!("Initializing ArbOpen leverage guard...");
            let leverage_guard_redis = RedisSettings::default();
            LeverageGuard::initialize(
                &leverage_guard_redis,
                dir_prefix.clone(),
                arb_mode,
                open_venue,
                hedge_venue,
                binance_account_mode,
            )
            .await?;
            info!("ArbOpen leverage guard initialized");

            info!("Initializing Binance FR position-limit guard...");
            BinanceFrPositionLimitGuard::initialize(
                &leverage_guard_redis,
                dir_prefix.clone(),
                arb_mode,
                open_venue,
                hedge_venue,
                binance_account_mode,
            )
            .await?;
            info!("Binance FR position-limit guard initialized");

            info!("Initializing Gate risk-limit guard...");
            GateFrRiskLimitGuard::initialize(
                &leverage_guard_redis,
                dir_prefix.clone(),
                arb_mode,
                open_venue,
                hedge_venue,
            )
            .await?;
            info!("Gate risk-limit guard initialized");

            info!("Initializing Bitget position-tier guard...");
            BitgetPositionTierGuard::initialize(
                &leverage_guard_redis,
                dir_prefix.clone(),
                arb_mode,
                open_venue,
                hedge_venue,
            )
            .await?;
            info!("Bitget position-tier guard initialized");

            // 5. 初始化 SignalChannel
            info!("Initializing SignalChannel singleton...");
            SignalChannel::initialize(DEFAULT_SIGNAL_CHANNEL, Some(DEFAULT_BACKWARD_CHANNEL))?;
            info!(
                "SignalChannel initialized on channel: {} backward_channel: {}",
                DEFAULT_SIGNAL_CHANNEL, DEFAULT_BACKWARD_CHANNEL
            );

            // 6. 初始化 ResampleChannel
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

            if !fast_poll {
                ResampleChannel::start_exposure_table_printer(Duration::from_secs(10));
            }

            // 7. 初始化 TradeEngHub（按 open/hedge 需求注册交易所）
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

            // 7.1 初始化 QueryEngHub（查询请求/响应通道）
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

            let enable_binance_std_usdt_rebalance = arb_mode == ArbMode::IntraArb
                && open_venue == TradingVenue::BinanceMargin
                && hedge_venue == TradingVenue::BinanceFutures
                && matches!(binance_account_mode, Some(BinanceAccountMode::Standard));
            if enable_binance_std_usdt_rebalance {
                if let Err(err) = RebalanceUsdtService::initialize(RebalanceUsdtConfig::from_env())
                {
                    warn!("Binance std USDT rebalance disabled: {err:#}");
                }
            } else if open_venue == TradingVenue::BinanceMargin
                || hedge_venue == TradingVenue::BinanceFutures
            {
                info!(
                    "Binance std USDT rebalance disabled: arb_mode={} open={:?} hedge={:?} account_mode={:?}",
                    arb_mode.as_str(),
                    open_venue,
                    hedge_venue,
                    binance_account_mode
                );
            }

            // 7.2 启动时执行一次账户快照查询（用于补齐/初始化本地风控状态）
            let snapshot_query = SnapshotQueryConfig::new(
                open_venue,
                hedge_venue,
                binance_account_mode,
            );
            if !fast_poll {
                let snapshot_query = snapshot_query.clone();
                tokio::task::spawn_local(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(60));

                    // Run once at startup.
                    publish_snapshot_queries(&snapshot_query);

                    // interval.tick() returns immediately on first call; consume it to avoid a duplicate send.
                    interval.tick().await;

                    // Re-run every 1 minute.
                    loop {
                        interval.tick().await;
                        publish_snapshot_queries(&snapshot_query);
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
            let mut pre_trade = PreTrade::new()
                .with_param_refresh(param_refresh)
                .with_snapshot_query(snapshot_query);
            if let Some(config) = intra_bwd_refresh {
                pre_trade = pre_trade.with_intra_bwd_refresh(config);
            }
            if let Some(config) = taker_decision_model_refresh {
                pre_trade = pre_trade.with_taker_decision_model_refresh(config);
            }
            if let Some(service) = auto_repay_service {
                pre_trade = pre_trade.with_auto_repay(service);
            }
            if let Some(service) = auto_collection_service {
                pre_trade = pre_trade.with_auto_collection(service);
            }
            pre_trade.run().await
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_arb_mode_uses_dir_namespace_before_venue_shape() {
        assert_eq!(
            infer_arb_mode_from_dir_name("binance_fr_arb03"),
            Some(ArbMode::FundingArb)
        );
        assert_eq!(
            infer_venues_from_dir_name("binance_fr_arb03"),
            Some((TradingVenue::BinanceMargin, TradingVenue::BinanceFutures))
        );
        assert_eq!(
            ArbMode::from_venues(TradingVenue::BinanceMargin, TradingVenue::BinanceFutures),
            ArbMode::IntraArb
        );
        assert_eq!(
            infer_arb_mode_from_dir_name("binance-intra-arb03"),
            Some(ArbMode::IntraArb)
        );
    }
}
