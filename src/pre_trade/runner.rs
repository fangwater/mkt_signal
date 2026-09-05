use crate::pre_trade::account_open_block::drive_account_open_block_capacity_poll;
use crate::pre_trade::auto_collection_service::AutoCollectionService;
use crate::pre_trade::auto_repay_service::AutoRepayService;
use crate::pre_trade::fr_position_concentration_guard::FrPositionConcentrationGuard;
use crate::pre_trade::hyperliquid_account_hash_from_env;
use crate::pre_trade::intra_bwd_symbol_list::IntraBwdSymbolList;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateLimiter;
use crate::pre_trade::order_queue_position_channel::OrderQueuePositionChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::query_eng_channel::QueryEngHub;
use crate::pre_trade::reactor_latency::{record_stage_latency, ReactorStage};
use crate::pre_trade::resample_channel::ResampleChannel;
use crate::pre_trade::runtime_flags::{enable_ipc_fast_poll, suppress_pre_submit_hot_path_logs};
use crate::pre_trade::signal_channel::{OpenSignalDropReason, SignalChannel};
use crate::pre_trade::signal_throttle::log_active_signal_throttles;
use crate::pre_trade::taker_decision_model::PreTradeTakerDecisionModel;
use crate::pre_trade::trade_eng_channel::TradeEngHub;
use crate::pre_trade::unimmr_force_close::UnimmrForceClose;
use crate::pre_trade::unimmr_open_lock::UnimmrOpenLock;
use crate::strategy::{OrphanStrategyManager, StrategyManager};
use account_common::BinanceAccountMode;
use anyhow::Result;
use bytes::Bytes;
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::redis_client::RedisSettings;
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use trade_engine::query_request::{GenericQueryRequest, HyperliquidQueryParams, QueryRequestType};

const PARAM_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const SNAPSHOT_QUERY_INTERVAL: Duration = Duration::from_secs(60);
const EXPOSURE_TABLE_PRINT_INTERVAL: Duration = Duration::from_secs(10);
const NON_FAST_POLL_IDLE_SLEEP: Duration = Duration::from_millis(1);
const ORDER_POSITION_POLL_INTERVAL: Duration = Duration::from_millis(1);
const ORDER_POSITION_DRAIN_LIMIT: usize = 64;
const HYPERLIQUID_PERIODIC_QUERIES: [(QueryRequestType, &str); 1] = [(
    QueryRequestType::HyperliquidUserAbstraction,
    "hyperliquid account abstraction mode",
)];

#[derive(Clone, Copy, Debug)]
struct FastPollDispatchBudgets {
    signal: usize,
    trade_resp: usize,
    monitor_state: usize,
    query_resp: usize,
    model_update: usize,
    period_strategy: usize,
    period_orphan: usize,
}

impl FastPollDispatchBudgets {
    fn from_env() -> Self {
        Self {
            signal: fast_poll_budget("PRE_TRADE_FAST_SIGNAL_BUDGET", 8),
            trade_resp: fast_poll_budget("PRE_TRADE_FAST_TRADE_RESP_BUDGET", 8),
            monitor_state: fast_poll_budget("PRE_TRADE_FAST_MONITOR_STATE_BUDGET", 8),
            query_resp: fast_poll_budget("PRE_TRADE_FAST_QUERY_RESP_BUDGET", 8),
            model_update: fast_poll_budget("PRE_TRADE_FAST_MODEL_UPDATE_BUDGET", 8),
            period_strategy: fast_poll_budget("PRE_TRADE_FAST_PERIOD_STRATEGY_BUDGET", 8),
            period_orphan: fast_poll_budget("PRE_TRADE_FAST_PERIOD_ORPHAN_BUDGET", 8),
        }
    }
}

#[derive(Clone)]
pub struct ParamRefreshConfig {
    redis: RedisSettings,
    env_name: Option<String>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
}

impl ParamRefreshConfig {
    pub fn new(
        redis: RedisSettings,
        env_name: Option<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Self {
        Self {
            redis,
            env_name,
            open_venue,
            hedge_venue,
        }
    }
}

#[derive(Clone)]
pub struct IntraBwdRefreshConfig {
    redis: RedisSettings,
    env_name: String,
    key_suffix: String,
}

impl IntraBwdRefreshConfig {
    pub fn new(redis: RedisSettings, env_name: String, key_suffix: String) -> Self {
        Self {
            redis,
            env_name,
            key_suffix,
        }
    }
}

#[derive(Clone)]
pub struct TakerDecisionModelRefreshConfig {
    redis: RedisSettings,
    namespace: Option<String>,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
}

impl TakerDecisionModelRefreshConfig {
    pub fn new(
        redis: RedisSettings,
        namespace: Option<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Self {
        Self {
            redis,
            namespace,
            open_venue,
            hedge_venue,
        }
    }
}

#[derive(Clone)]
pub struct SnapshotQueryConfig {
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
    include_binance_spot_snapshot: bool,
}

impl SnapshotQueryConfig {
    pub fn new(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        binance_account_mode: Option<BinanceAccountMode>,
    ) -> Self {
        Self {
            open_venue,
            hedge_venue,
            binance_account_mode,
            include_binance_spot_snapshot: true,
        }
    }

    pub fn new_exec(venue: TradingVenue, binance_account_mode: Option<BinanceAccountMode>) -> Self {
        Self {
            open_venue: venue,
            hedge_venue: venue,
            binance_account_mode,
            include_binance_spot_snapshot: false,
        }
    }
}

pub struct PreTrade {
    param_refresh: Option<ParamRefreshConfig>,
    intra_bwd_refresh: Option<IntraBwdRefreshConfig>,
    taker_decision_model_refresh: Option<TakerDecisionModelRefreshConfig>,
    snapshot_query: Option<SnapshotQueryConfig>,
    order_queue_position: Option<OrderQueuePositionChannel>,
    auto_repay: Option<AutoRepayService>,
    auto_collection: Option<AutoCollectionService>,
    publish_legacy_resample: bool,
}

fn drive_strategy_manager_period_clock_rc(
    strategy_mgr: &Rc<RefCell<StrategyManager>>,
    now: i64,
) -> usize {
    drive_strategy_manager_period_clock_rc_limit(strategy_mgr, now, usize::MAX)
}

fn drive_strategy_manager_period_clock_rc_limit(
    strategy_mgr: &Rc<RefCell<StrategyManager>>,
    now: i64,
    max_inspect: usize,
) -> usize {
    let iterations = strategy_mgr.borrow().len().min(max_inspect);
    let mut inspected = 0usize;
    for _ in 0..iterations {
        let strategy_opt = { strategy_mgr.borrow_mut().take_next_queued() };
        let Some(mut strategy) = strategy_opt else {
            break;
        };
        inspected += 1;
        strategy.handle_period_clock(now);
        if strategy.is_active() {
            strategy_mgr.borrow_mut().insert(strategy);
        }
    }
    inspected
}

fn drive_orphan_manager_period_clock_rc(
    orphan_strategy_mgr: &Rc<RefCell<OrphanStrategyManager>>,
    now: i64,
) -> usize {
    drive_orphan_manager_period_clock_rc_limit(orphan_strategy_mgr, now, usize::MAX)
}

fn drive_orphan_manager_period_clock_rc_limit(
    orphan_strategy_mgr: &Rc<RefCell<OrphanStrategyManager>>,
    now: i64,
    max_inspect: usize,
) -> usize {
    let iterations = orphan_strategy_mgr.borrow().len().min(max_inspect);
    let mut inspected = 0usize;
    for _ in 0..iterations {
        let strategy_opt = { orphan_strategy_mgr.borrow_mut().take_next_queued() };
        let Some(mut strategy) = strategy_opt else {
            break;
        };
        inspected += 1;
        strategy.handle_period_clock(now);
        if strategy.is_active() {
            orphan_strategy_mgr.borrow_mut().insert(strategy);
        }
    }
    inspected
}

fn drive_strategy_manager_period_clock(now: i64) {
    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
    let _ = drive_strategy_manager_period_clock_rc(&strategy_mgr, now);
}

fn drive_strategy_manager_period_clock_limit(now: i64, max_inspect: usize) -> usize {
    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
    drive_strategy_manager_period_clock_rc_limit(&strategy_mgr, now, max_inspect)
}

fn drive_orphan_manager_period_clock(now: i64) {
    let orphan_strategy_mgr = MonitorChannel::instance().orphan_strategy_mgr();
    let _ = drive_orphan_manager_period_clock_rc(&orphan_strategy_mgr, now);
}

fn drive_orphan_manager_period_clock_limit(now: i64, max_inspect: usize) -> usize {
    let orphan_strategy_mgr = MonitorChannel::instance().orphan_strategy_mgr();
    drive_orphan_manager_period_clock_rc_limit(&orphan_strategy_mgr, now, max_inspect)
}

pub fn publish_snapshot_queries(config: &SnapshotQueryConfig) -> bool {
    let open_venue = config.open_venue;
    let hedge_venue = config.hedge_venue;
    let need_binance = open_venue.trade_engine_exchange() == "binance"
        || hedge_venue.trade_engine_exchange() == "binance";
    let need_binance_um = matches!(open_venue, TradingVenue::BinanceFutures)
        || matches!(hedge_venue, TradingVenue::BinanceFutures);
    let need_binance_cm = matches!(open_venue, TradingVenue::BinanceCoinFutures)
        || matches!(hedge_venue, TradingVenue::BinanceCoinFutures);
    let need_binance_spot = matches!(open_venue, TradingVenue::BinanceMargin)
        || matches!(hedge_venue, TradingVenue::BinanceMargin);
    let need_okex = open_venue.trade_engine_exchange() == "okex"
        || hedge_venue.trade_engine_exchange() == "okex";
    let need_gate = open_venue.trade_engine_exchange() == "gate"
        || hedge_venue.trade_engine_exchange() == "gate";
    let need_bybit = open_venue.trade_engine_exchange() == "bybit"
        || hedge_venue.trade_engine_exchange() == "bybit";
    let need_bitget = open_venue.trade_engine_exchange() == "bitget"
        || hedge_venue.trade_engine_exchange() == "bitget";
    let need_bitget_usdt = matches!(open_venue, TradingVenue::BitgetFutures)
        || matches!(hedge_venue, TradingVenue::BitgetFutures);
    let need_bitget_coin = matches!(open_venue, TradingVenue::BitgetCoinFutures)
        || matches!(hedge_venue, TradingVenue::BitgetCoinFutures);
    let need_hyperliquid = open_venue.trade_engine_exchange() == "hyperliquid"
        || hedge_venue.trade_engine_exchange() == "hyperliquid";

    if !need_binance && !need_okex && !need_gate && !need_bybit && !need_bitget && !need_hyperliquid
    {
        return false;
    }

    let mut published = false;
    let binance_is_standard = matches!(
        config.binance_account_mode,
        Some(BinanceAccountMode::Standard)
    );
    let mut publish =
        |exchange: &str, request_type: QueryRequestType, context: Bytes, desc: &str| {
            let now = get_timestamp_us();
            let req = GenericQueryRequest::create(request_type, now, now, context);
            let _ = QueryEngHub::publish_query_request(exchange, &req.to_bytes());
            info!("snapshot query sent: {desc}");
            published = true;
        };

    if need_binance {
        if binance_is_standard {
            if config.include_binance_spot_snapshot && need_binance_spot {
                publish(
                    "binance",
                    QueryRequestType::BinanceSpotAccountSnapshotStd,
                    Bytes::new(),
                    "binance spot account snapshot (standard)",
                );
            }
            if need_binance_um {
                publish(
                    "binance",
                    QueryRequestType::BinanceUmBalanceSnapshotStd,
                    Bytes::new(),
                    "binance UM balance snapshot (standard)",
                );
                publish(
                    "binance",
                    QueryRequestType::BinanceUmAccountSnapshotStd,
                    Bytes::new(),
                    "binance UM account snapshot (standard)",
                );
            }
            if need_binance_cm {
                publish(
                    "binance",
                    QueryRequestType::BinanceCmBalanceSnapshotStd,
                    Bytes::new(),
                    "binance CM balance snapshot (standard)",
                );
                publish(
                    "binance",
                    QueryRequestType::BinanceCmAccountSnapshotStd,
                    Bytes::new(),
                    "binance CM account snapshot (standard)",
                );
            }
        } else {
            publish(
                "binance",
                QueryRequestType::BinancePmBalanceSnapshot,
                Bytes::new(),
                "binance PM balance snapshot",
            );
            if need_binance_um {
                publish(
                    "binance",
                    QueryRequestType::BinanceUmAccountSnapshot,
                    Bytes::new(),
                    "binance UM account snapshot",
                );
            }
            if need_binance_cm {
                publish(
                    "binance",
                    QueryRequestType::BinancePmCmAccountSnapshot,
                    Bytes::new(),
                    "binance PM CM account snapshot",
                );
            }
        }
    }
    if need_okex {
        publish(
            "okex",
            QueryRequestType::OkexAccountBalanceSnapshot,
            Bytes::new(),
            "okex account balance snapshot",
        );
        publish(
            "okex",
            QueryRequestType::OkexPositionsSnapshot,
            Bytes::from_static(b"instType=SWAP"),
            "okex positions snapshot (instType=SWAP)",
        );
    }
    if need_gate {
        publish(
            "gate",
            QueryRequestType::GateUnifiedBalanceSnapshot,
            Bytes::new(),
            "gate unified balance snapshot",
        );
        publish(
            "gate",
            QueryRequestType::GateUnifiedPositionsSnapshot,
            Bytes::new(),
            "gate futures positions snapshot (includes upl)",
        );
    }
    if need_bybit {
        publish(
            "bybit",
            QueryRequestType::BybitAccountBalanceSnapshot,
            Bytes::from_static(b"accountType=UNIFIED"),
            "bybit unified wallet balance snapshot",
        );
        publish(
            "bybit",
            QueryRequestType::BybitPositionsSnapshot,
            Bytes::from_static(b"category=linear&settleCoin=USDT&limit=200"),
            "bybit linear positions snapshot",
        );
    }
    if need_bitget {
        publish(
            "bitget",
            QueryRequestType::BitgetAccountBalanceSnapshot,
            Bytes::new(),
            "bitget unified account balance snapshot",
        );
        if need_bitget_usdt {
            publish(
                "bitget",
                QueryRequestType::BitgetPositionsSnapshot,
                Bytes::from_static(b"category=USDT-FUTURES"),
                "bitget UTA USDT-FUTURES positions snapshot",
            );
        }
        if need_bitget_coin {
            publish(
                "bitget",
                QueryRequestType::BitgetCoinPositionsSnapshot,
                Bytes::from_static(b"category=COIN-FUTURES"),
                "bitget UTA COIN-FUTURES positions snapshot",
            );
        }
    }
    if need_hyperliquid {
        match hyperliquid_account_hash_from_env() {
            Ok(account_hash) => {
                let params =
                    || HyperliquidQueryParams::create(account_hash, Bytes::new()).to_bytes();
                // Account balances and positions have one writer: the authenticated private
                // account stream. The periodic query only verifies that account abstraction
                // mode has not drifted underneath the trading client.
                for (request_type, description) in HYPERLIQUID_PERIODIC_QUERIES {
                    publish("hyperliquid", request_type, params(), description);
                }
            }
            Err(err) => warn!("skip Hyperliquid snapshot queries: {err}"),
        }
    }
    published
}

fn select_slower_open_drop_reason(
    lhs: Option<OpenSignalDropReason>,
    rhs: Option<OpenSignalDropReason>,
) -> Option<OpenSignalDropReason> {
    match (lhs, rhs) {
        (Some(left), Some(right)) => {
            if right.elapsed_us > left.elapsed_us {
                Some(right)
            } else {
                Some(left)
            }
        }
        (Some(reason), None) | (None, Some(reason)) => Some(reason),
        (None, None) => None,
    }
}

fn fast_poll_budget(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

fn reactor_idle_spin_iters(fast_poll: bool) -> usize {
    if !fast_poll {
        return 0;
    }

    std::env::var("PRE_TRADE_REACTOR_IDLE_SPIN_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1024)
}

impl Default for PreTrade {
    fn default() -> Self {
        Self::new()
    }
}

impl PreTrade {
    pub fn new() -> Self {
        Self {
            param_refresh: None,
            intra_bwd_refresh: None,
            taker_decision_model_refresh: None,
            snapshot_query: None,
            order_queue_position: None,
            auto_repay: None,
            auto_collection: None,
            publish_legacy_resample: true,
        }
    }

    pub fn with_param_refresh(mut self, config: ParamRefreshConfig) -> Self {
        self.param_refresh = Some(config);
        self
    }

    pub fn with_intra_bwd_refresh(mut self, config: IntraBwdRefreshConfig) -> Self {
        self.intra_bwd_refresh = Some(config);
        self
    }

    pub fn with_taker_decision_model_refresh(
        mut self,
        config: TakerDecisionModelRefreshConfig,
    ) -> Self {
        self.taker_decision_model_refresh = Some(config);
        self
    }

    pub fn with_snapshot_query(mut self, config: SnapshotQueryConfig) -> Self {
        self.snapshot_query = Some(config);
        self
    }

    pub fn with_order_queue_position(mut self, channel: OrderQueuePositionChannel) -> Self {
        self.order_queue_position = Some(channel);
        self
    }

    pub fn with_auto_repay(mut self, service: AutoRepayService) -> Self {
        self.auto_repay = Some(service);
        self
    }

    pub fn with_auto_collection(mut self, service: AutoCollectionService) -> Self {
        self.auto_collection = Some(service);
        self
    }

    pub fn without_legacy_resample(mut self) -> Self {
        self.publish_legacy_resample = false;
        self
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade main loop starting");
        let param_refresh = self.param_refresh;
        let intra_bwd_refresh = self.intra_bwd_refresh;
        let taker_decision_model_refresh = self.taker_decision_model_refresh;
        let snapshot_query = self.snapshot_query;
        let mut order_queue_position = self.order_queue_position;
        let mut auto_repay = self.auto_repay;
        let mut auto_collection = self.auto_collection;
        let publish_legacy_resample = self.publish_legacy_resample;

        // 定时器状态
        let resample_interval = std::time::Duration::from_secs(3);
        let mut next_resample = std::time::Instant::now() + resample_interval;
        let mut next_exposure_table_print =
            std::time::Instant::now() + EXPOSURE_TABLE_PRINT_INTERVAL;
        let mut next_order_position_poll = std::time::Instant::now() + ORDER_POSITION_POLL_INTERVAL;
        let throttle_log_interval_secs =
            std::env::var("PRE_TRADE_SIGNAL_THROTTLE_LOG_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(60);
        let throttle_log_interval = std::time::Duration::from_secs(throttle_log_interval_secs);
        let mut next_throttle_log = std::time::Instant::now() + throttle_log_interval;
        let order_rate_cleanup_interval = std::time::Duration::from_secs(10);
        let mut next_order_rate_cleanup = std::time::Instant::now() + order_rate_cleanup_interval;
        let arb_startup_net_log_interval = std::time::Duration::from_secs(30);
        let mut next_arb_startup_net_log = std::time::Instant::now() + arb_startup_net_log_interval;
        let account_open_block_poll_interval = std::time::Duration::from_secs(60);
        let mut next_account_open_block_poll =
            std::time::Instant::now() + account_open_block_poll_interval;
        let mut next_param_refresh = Instant::now();
        let fast_poll = enable_ipc_fast_poll();
        let fast_poll_budgets = FastPollDispatchBudgets::from_env();
        if let Some(refresh_cfg) = intra_bwd_refresh.as_ref() {
            IntraBwdSymbolList::start_background_refresh(
                refresh_cfg.redis.clone(),
                refresh_cfg.env_name.clone(),
                refresh_cfg.key_suffix.clone(),
            );
        }
        if let Some(refresh_cfg) = taker_decision_model_refresh.as_ref() {
            PreTradeTakerDecisionModel::start_config_background_refresh(
                refresh_cfg.redis.clone(),
                refresh_cfg.namespace.clone(),
                refresh_cfg.open_venue,
                refresh_cfg.hedge_venue,
            );
        }
        if let Some(auto_repay) = auto_repay.take() {
            auto_repay.start();
        }
        if let Some(auto_collection) = auto_collection.take() {
            auto_collection.start_startup_and_daily_task();
        }
        let mut next_snapshot_query = Instant::now();
        let mut last_loop_end_us = get_timestamp_us();
        let mut pending_maintenance_open_drop_reason: Option<OpenSignalDropReason> = None;
        info!(
            "pre_trade signal throttle log started (interval={}s)",
            throttle_log_interval_secs
        );
        info!("pre_trade MM open order rate cleanup started (interval=10s window=60s)");
        info!(
            "pre_trade param refresh configured (enable_ipc_fast_poll={} synchronous_main_loop_refresh={} taker_model_refresh={} interval_s={})",
            fast_poll,
            param_refresh.is_some(),
            taker_decision_model_refresh.is_some(),
            PARAM_REFRESH_INTERVAL.as_secs()
        );

        // 周期检查频率设为 20ms，提高 MM trigger 响应及时性，同时保持较低调度开销。
        // IPC hot path 不等待这个 tick；fast poll 关闭时空闲 1ms，充分让出 CPU。
        let period_clock_interval = Duration::from_millis(20);
        let mut next_period_clock = Instant::now();
        let mut pending_period_strategy_inspect = 0usize;
        let mut pending_period_orphan_inspect = 0usize;
        let idle_spin_iters = reactor_idle_spin_iters(fast_poll);
        let mut idle_spin_count = 0usize;
        if fast_poll {
            info!(
                "pre_trade reactor idle configured (enable_ipc_fast_poll=true spin_iters={} idle_policy=yield)",
                idle_spin_iters
            );
        } else {
            info!(
                "pre_trade reactor idle configured (enable_ipc_fast_poll=false spin_iters=0 idle_policy=sleep sleep_ms={})",
                NON_FAST_POLL_IDLE_SLEEP.as_millis()
            );
        }
        info!(
            "pre_trade hot-path log suppression configured (suppress_pre_submit_hot_path_logs={})",
            suppress_pre_submit_hot_path_logs()
        );
        if fast_poll {
            info!(
                "pre_trade fast-poll dispatch budgets configured signal={} trade_resp={} monitor_state={} query_resp={} model_update={} period_strategy={} period_orphan={}",
                fast_poll_budgets.signal,
                fast_poll_budgets.trade_resp,
                fast_poll_budgets.monitor_state,
                fast_poll_budgets.query_resp,
                fast_poll_budgets.model_update,
                fast_poll_budgets.period_strategy,
                fast_poll_budgets.period_orphan
            );
        }

        loop {
            let loop_start_us = get_timestamp_us();
            let param_refresh_due = param_refresh
                .as_ref()
                .is_some_and(|_| Instant::now() >= next_param_refresh);
            let open_drop_reason = pending_maintenance_open_drop_reason.take();
            if param_refresh_due {
                let maintenance_start_us = get_timestamp_us();
                let pre_refresh_drop_reason = select_slower_open_drop_reason(
                    open_drop_reason,
                    Some(OpenSignalDropReason {
                        source: "synchronous_param_refresh",
                        elapsed_us: 0,
                        threshold_us: 0,
                    }),
                );
                MonitorChannel::drain_pending_state_updates_with_refresh();
                SignalChannel::drain_pending_with_open_drop(pre_refresh_drop_reason);

                let refresh_cfg = param_refresh
                    .as_ref()
                    .expect("param refresh config exists when refresh is due");
                let loader = PreTradeParamsLoader::instance();
                match loader.load_from_redis_blocking(
                    &refresh_cfg.redis,
                    refresh_cfg.env_name.as_deref(),
                    refresh_cfg.open_venue,
                    refresh_cfg.hedge_venue,
                ) {
                    Ok(()) => info!("pre_trade risk parameters synchronous refresh succeeded"),
                    Err(err) => {
                        warn!("pre_trade risk parameters synchronous refresh failed: {err:#}")
                    }
                }
                if let Err(err) = FrPositionConcentrationGuard::refresh_blocking() {
                    warn!("FR position concentration synchronous refresh failed: {err:#}");
                }
                let unimmr_cancelled = UnimmrOpenLock::cancel_recovered_fr_closes();
                if unimmr_cancelled > 0 {
                    info!(
                        "UniMMR recover close cancel submitted: strategies={}",
                        unimmr_cancelled
                    );
                }
                if let Err(err) = UnimmrOpenLock::flush_notification_blocking() {
                    warn!("UniMMR risk notification failed: {err:#}");
                }

                let maintenance_finished = Instant::now();
                while maintenance_finished >= next_param_refresh {
                    next_param_refresh += PARAM_REFRESH_INTERVAL;
                }
                let maintenance_elapsed_us =
                    get_timestamp_us().saturating_sub(maintenance_start_us);
                let refresh_drop_reason = select_slower_open_drop_reason(
                    pre_refresh_drop_reason,
                    Some(OpenSignalDropReason {
                        source: "synchronous_param_refresh",
                        elapsed_us: maintenance_elapsed_us,
                        threshold_us: 0,
                    }),
                );
                info!(
                    "pre_trade synchronous maintenance round finished: elapsed_us={} open_signals=drop close_signals=preserve",
                    maintenance_elapsed_us
                );
                pending_maintenance_open_drop_reason = refresh_drop_reason;
                last_loop_end_us = get_timestamp_us();
                continue;
            }
            let mut next_loop_open_drop_reason = None;

            let mut has_work = false;
            macro_rules! finish_fast_poll_work {
                ($drop_reason:expr) => {{
                    idle_spin_count = 0;
                    let loop_end_us = get_timestamp_us();
                    record_stage_latency(ReactorStage::PreviousLoop, loop_start_us, loop_end_us);
                    pending_maintenance_open_drop_reason = $drop_reason;
                    last_loop_end_us = loop_end_us;
                    continue;
                }};
            }

            // 优先级：trade_resp / monitor_state（成交回报 -> taker 对冲触发）排在
            // 新开仓 signal 之前。maker 成交是低频事件，抢占对 signal p50 影响可忽略，
            // 但避免 fill 在 signal 批次后排队（对冲链路 p99 直接受益）。
            // open_drop_reason 先行合并，保证 fill/响应提前 finish 时不丢失该轮的
            // 维护窗口丢弃标记。
            if fast_poll {
                next_loop_open_drop_reason =
                    select_slower_open_drop_reason(next_loop_open_drop_reason, open_drop_reason);
            }

            if fast_poll {
                if TradeEngHub::drain_pending_responses_limit(fast_poll_budgets.trade_resp) {
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
            } else {
                has_work |= TradeEngHub::drain_pending_responses();
            }

            let monitor_has_work = if fast_poll {
                MonitorChannel::drain_pending_state_updates_limit(fast_poll_budgets.monitor_state)
            } else {
                MonitorChannel::drain_pending_state_updates_with_refresh().0
            };
            has_work |= monitor_has_work;
            if fast_poll && monitor_has_work {
                let refresh_start_us = get_timestamp_us();
                if MonitorChannel::refresh_basic_state_if_due_after_fast_poll(true) {
                    let refresh_elapsed_us = get_timestamp_us().saturating_sub(refresh_start_us);
                    next_loop_open_drop_reason = select_slower_open_drop_reason(
                        next_loop_open_drop_reason,
                        Some(OpenSignalDropReason {
                            source: "basic_state_refresh",
                            elapsed_us: refresh_elapsed_us,
                            threshold_us: 0,
                        }),
                    );
                }
                finish_fast_poll_work!(next_loop_open_drop_reason);
            }

            if fast_poll {
                let before_signal_us = get_timestamp_us();
                let (signal_has_work, signal_budget_exhausted) =
                    SignalChannel::drain_pending_with_open_drop_limit(
                        open_drop_reason,
                        fast_poll_budgets.signal,
                    );
                if signal_has_work {
                    record_stage_latency(ReactorStage::ReactorGap, last_loop_end_us, loop_start_us);
                    record_stage_latency(
                        ReactorStage::BeforeSignal,
                        loop_start_us,
                        before_signal_us,
                    );
                    if signal_budget_exhausted {
                        finish_fast_poll_work!(open_drop_reason);
                    }
                    finish_fast_poll_work!(None);
                }
            }

            if fast_poll {
                if QueryEngHub::drain_pending_responses_limit(fast_poll_budgets.query_resp) {
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
            } else {
                has_work |= QueryEngHub::drain_pending_responses();
            }

            let instant_now = Instant::now();
            if fast_poll {
                if let Some(snapshot_cfg) = snapshot_query.as_ref() {
                    if instant_now >= next_snapshot_query {
                        let snapshot_start_us = get_timestamp_us();
                        let published = publish_snapshot_queries(snapshot_cfg);
                        if published {
                            let snapshot_elapsed_us =
                                get_timestamp_us().saturating_sub(snapshot_start_us);
                            next_loop_open_drop_reason = select_slower_open_drop_reason(
                                next_loop_open_drop_reason,
                                Some(OpenSignalDropReason {
                                    source: "snapshot_query",
                                    elapsed_us: snapshot_elapsed_us,
                                    threshold_us: 0,
                                }),
                            );
                        }
                        while instant_now >= next_snapshot_query {
                            next_snapshot_query += SNAPSHOT_QUERY_INTERVAL;
                        }
                        finish_fast_poll_work!(next_loop_open_drop_reason);
                    }
                }
                let refresh_start_us = get_timestamp_us();
                if MonitorChannel::refresh_basic_state_if_due_after_fast_poll(false) {
                    let refresh_elapsed_us = get_timestamp_us().saturating_sub(refresh_start_us);
                    next_loop_open_drop_reason = select_slower_open_drop_reason(
                        next_loop_open_drop_reason,
                        Some(OpenSignalDropReason {
                            source: "basic_state_refresh",
                            elapsed_us: refresh_elapsed_us,
                            threshold_us: 0,
                        }),
                    );
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
            }

            if fast_poll {
                let before_signal_us = get_timestamp_us();
                let (signal_has_work, signal_budget_exhausted) =
                    SignalChannel::drain_pending_with_open_drop_limit(
                        next_loop_open_drop_reason,
                        fast_poll_budgets.signal,
                    );
                if signal_has_work {
                    record_stage_latency(ReactorStage::ReactorGap, last_loop_end_us, loop_start_us);
                    record_stage_latency(
                        ReactorStage::BeforeSignal,
                        loop_start_us,
                        before_signal_us,
                    );
                    if signal_budget_exhausted {
                        finish_fast_poll_work!(next_loop_open_drop_reason);
                    }
                    finish_fast_poll_work!(None);
                }
            } else {
                has_work |= SignalChannel::drain_pending_with_open_drop(open_drop_reason);
            }

            let force_close_activated = UnimmrForceClose::drive(get_timestamp_us());
            if force_close_activated > 0 {
                has_work = true;
            }

            if let Some(transition) = PreTradeTakerDecisionModel::take_transition_global() {
                has_work = true;
                let now = get_timestamp_us();
                let triggered = MonitorChannel::instance()
                    .strategy_mgr()
                    .borrow_mut()
                    .trigger_all_arb_hedge_lazy_taker_on_model_transition(now);
                warn!(
                    "pre_trade taker decision model transition: previous_service={} next_service={} reason={} direct_taker_triggered={}",
                    transition.previous_service,
                    transition.next_service.as_deref().unwrap_or("-"),
                    transition.reason,
                    triggered
                );
            }

            let model_updates = if fast_poll {
                PreTradeTakerDecisionModel::poll_updates_global_limit(
                    fast_poll_budgets.model_update,
                )
            } else {
                PreTradeTakerDecisionModel::poll_updates_global()
            };
            if !model_updates.is_empty() {
                has_work = true;
                let now = get_timestamp_us();
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let mut mgr = strategy_mgr.borrow_mut();
                for update in model_updates {
                    let _ = mgr.trigger_arb_hedge_lazy_taker_on_model_update(
                        &update.symbol,
                        now,
                        update.percentile,
                    );
                    let _ = mgr.trigger_arb_open_cancel_on_model_update(&update.symbol, now);
                }
                if fast_poll {
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
            }

            let instant_now = Instant::now();
            let mut ran_periodic = false;
            if fast_poll {
                if pending_period_strategy_inspect == 0
                    && pending_period_orphan_inspect == 0
                    && instant_now >= next_period_clock
                {
                    pending_period_strategy_inspect =
                        MonitorChannel::instance().strategy_mgr().borrow().len();
                    pending_period_orphan_inspect = MonitorChannel::instance()
                        .orphan_strategy_mgr()
                        .borrow()
                        .len();
                    while instant_now >= next_period_clock {
                        next_period_clock += period_clock_interval;
                    }
                }

                if pending_period_strategy_inspect > 0 || pending_period_orphan_inspect > 0 {
                    let periodic_start_us = get_timestamp_us();
                    let now = get_timestamp_us();
                    let strategy_budget =
                        pending_period_strategy_inspect.min(fast_poll_budgets.period_strategy);
                    let strategy_inspected =
                        drive_strategy_manager_period_clock_limit(now, strategy_budget);
                    if strategy_inspected < strategy_budget {
                        pending_period_strategy_inspect = 0;
                    } else {
                        pending_period_strategy_inspect =
                            pending_period_strategy_inspect.saturating_sub(strategy_inspected);
                    }

                    let orphan_budget =
                        pending_period_orphan_inspect.min(fast_poll_budgets.period_orphan);
                    let orphan_inspected =
                        drive_orphan_manager_period_clock_limit(now, orphan_budget);
                    if orphan_inspected < orphan_budget {
                        pending_period_orphan_inspect = 0;
                    } else {
                        pending_period_orphan_inspect =
                            pending_period_orphan_inspect.saturating_sub(orphan_inspected);
                    }

                    record_stage_latency(
                        ReactorStage::Periodic,
                        periodic_start_us,
                        get_timestamp_us(),
                    );
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }

                if publish_legacy_resample && instant_now >= next_resample {
                    let result = ResampleChannel::with(|ch| ch.publish_resample_entries());
                    if let Err(err) = result {
                        warn!("pre_trade resample publish failed: {err:#}");
                        next_resample = Instant::now() + resample_interval;
                    } else {
                        while instant_now >= next_resample {
                            next_resample += resample_interval;
                        }
                    }
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }

                if publish_legacy_resample && instant_now >= next_exposure_table_print {
                    let exposure_table_print_start_us = get_timestamp_us();
                    ResampleChannel::with(|ch| ch.print_exposure_table_snapshot());
                    let exposure_table_print_elapsed_us =
                        get_timestamp_us().saturating_sub(exposure_table_print_start_us);
                    while instant_now >= next_exposure_table_print {
                        next_exposure_table_print += EXPOSURE_TABLE_PRINT_INTERVAL;
                    }
                    next_loop_open_drop_reason = select_slower_open_drop_reason(
                        next_loop_open_drop_reason,
                        Some(OpenSignalDropReason {
                            source: "exposure_table_print",
                            elapsed_us: exposure_table_print_elapsed_us,
                            threshold_us: 0,
                        }),
                    );
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }

                if instant_now >= next_throttle_log {
                    log_active_signal_throttles(50);
                    while instant_now >= next_throttle_log {
                        next_throttle_log += throttle_log_interval;
                    }
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }

                if instant_now >= next_order_rate_cleanup {
                    let now = get_timestamp_us();
                    OrderRateLimiter::cleanup_expired(now);
                    while instant_now >= next_order_rate_cleanup {
                        next_order_rate_cleanup += order_rate_cleanup_interval;
                    }
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }

                if instant_now >= next_account_open_block_poll {
                    let now = get_timestamp_us();
                    drive_account_open_block_capacity_poll(now);
                    while instant_now >= next_account_open_block_poll {
                        next_account_open_block_poll += account_open_block_poll_interval;
                    }
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }

                if instant_now >= next_arb_startup_net_log {
                    let status = MonitorChannel::instance().arb_startup_net_gate_status();
                    if status.enabled && !status.ready {
                        warn!(
                            "双边net还没有初始化: open_ready={} hedge_ready={} open_ts_us={} hedge_ts_us={} dropped_arb_signals={}",
                            status.open_ready,
                            status.hedge_ready,
                            status.open_ts_us,
                            status.hedge_ts_us,
                            status.dropped_signals
                        );
                    }
                    while instant_now >= next_arb_startup_net_log {
                        next_arb_startup_net_log += arb_startup_net_log_interval;
                    }
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
            } else if instant_now >= next_period_clock {
                ran_periodic = true;
                let periodic_start_us = get_timestamp_us();
                let now = get_timestamp_us();
                drive_strategy_manager_period_clock(now);
                drive_orphan_manager_period_clock(now);
                while instant_now >= next_period_clock {
                    next_period_clock += period_clock_interval;
                }

                // 发布重采样数据
                if publish_legacy_resample {
                    while instant_now >= next_resample {
                        let result = ResampleChannel::with(|ch| ch.publish_resample_entries());
                        if let Err(err) = result {
                            warn!("pre_trade resample publish failed: {err:#}");
                            next_resample = Instant::now() + resample_interval;
                            break;
                        }
                        next_resample += resample_interval;
                    }
                }

                while instant_now >= next_throttle_log {
                    log_active_signal_throttles(50);
                    next_throttle_log += throttle_log_interval;
                }

                while instant_now >= next_order_rate_cleanup {
                    OrderRateLimiter::cleanup_expired(now);
                    next_order_rate_cleanup += order_rate_cleanup_interval;
                }

                while instant_now >= next_account_open_block_poll {
                    drive_account_open_block_capacity_poll(now);
                    next_account_open_block_poll += account_open_block_poll_interval;
                }
                record_stage_latency(
                    ReactorStage::Periodic,
                    periodic_start_us,
                    get_timestamp_us(),
                );

                while instant_now >= next_arb_startup_net_log {
                    let status = MonitorChannel::instance().arb_startup_net_gate_status();
                    if status.enabled && !status.ready {
                        warn!(
                            "双边net还没有初始化: open_ready={} hedge_ready={} open_ts_us={} hedge_ts_us={} dropped_arb_signals={}",
                            status.open_ready,
                            status.hedge_ready,
                            status.open_ts_us,
                            status.hedge_ts_us,
                            status.dropped_signals
                        );
                    }
                    next_arb_startup_net_log += arb_startup_net_log_interval;
                }
            }

            if instant_now >= next_order_position_poll {
                let order_position_has_work = order_queue_position
                    .as_mut()
                    .is_some_and(|channel| channel.drain_pending_limit(ORDER_POSITION_DRAIN_LIMIT));
                has_work |= order_position_has_work;
                while instant_now >= next_order_position_poll {
                    next_order_position_poll += ORDER_POSITION_POLL_INTERVAL;
                }
                if fast_poll && order_position_has_work {
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
            }

            if has_work || ran_periodic {
                idle_spin_count = 0;
                let loop_end_us = get_timestamp_us();
                if fast_poll {
                    record_stage_latency(ReactorStage::PreviousLoop, loop_start_us, loop_end_us);
                    pending_maintenance_open_drop_reason = next_loop_open_drop_reason;
                }
                last_loop_end_us = loop_end_us;
                continue;
            }

            if fast_poll {
                if idle_spin_count < idle_spin_iters {
                    idle_spin_count += 1;
                    std::hint::spin_loop();
                    pending_maintenance_open_drop_reason = next_loop_open_drop_reason;
                    last_loop_end_us = get_timestamp_us();
                    continue;
                }
                idle_spin_count = 0;

                pending_maintenance_open_drop_reason = next_loop_open_drop_reason;
                last_loop_end_us = get_timestamp_us();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = tokio::task::yield_now() => {}
                }
            } else {
                idle_spin_count = 0;
                last_loop_end_us = get_timestamp_us();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = tokio::time::sleep(NON_FAST_POLL_IDLE_SLEEP) => {}
                }
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        drive_orphan_manager_period_clock_rc, drive_strategy_manager_period_clock_rc,
        reactor_idle_spin_iters, SnapshotQueryConfig, HYPERLIQUID_PERIODIC_QUERIES,
    };
    use crate::strategy::orphan_order_strategy::OrphanOrderStrategy;
    use crate::strategy::{OrphanStrategyManager, Strategy, StrategyManager};
    use account_common::BinanceAccountMode;
    use order_common::{OrderUpdate, TradeUpdate, TradingVenue};
    use signal_common::trade_signal::TradeSignal;
    use std::any::Any;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::{Mutex, OnceLock};
    use trade_engine::query_request::QueryRequestType;

    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn reactor_idle_spin_is_disabled_when_fast_poll_is_off() {
        let _guard = env_test_lock();
        std::env::set_var("PRE_TRADE_REACTOR_IDLE_SPIN_ITERS", "2048");
        assert_eq!(reactor_idle_spin_iters(false), 0);
        assert_eq!(reactor_idle_spin_iters(true), 2048);
        std::env::remove_var("PRE_TRADE_REACTOR_IDLE_SPIN_ITERS");
    }

    #[test]
    fn exec_snapshot_skips_binance_spot_but_intra_keeps_it() {
        let exec = SnapshotQueryConfig::new_exec(
            TradingVenue::BinanceFutures,
            Some(BinanceAccountMode::Standard),
        );
        assert!(!exec.include_binance_spot_snapshot);

        let intra = SnapshotQueryConfig::new(
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            Some(BinanceAccountMode::Standard),
        );
        assert!(intra.include_binance_spot_snapshot);
    }

    #[test]
    fn hyperliquid_periodic_query_only_validates_account_mode() {
        assert_eq!(
            HYPERLIQUID_PERIODIC_QUERIES
                .iter()
                .map(|(request_type, _)| *request_type)
                .collect::<Vec<_>>(),
            vec![QueryRequestType::HyperliquidUserAbstraction]
        );
    }

    struct ReentrantTickStrategy {
        id: i32,
        manager: Rc<RefCell<StrategyManager>>,
        tick_hits: Rc<Cell<u32>>,
        active: bool,
    }

    impl Strategy for ReentrantTickStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, _order_id: i64) -> bool {
            false
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {
            self.tick_hits.set(self.tick_hits.get() + 1);
            let _ = self.manager.borrow_mut().contains(self.id);
            self.active = false;
        }

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some("BTCUSDT")
        }
    }

    struct ReentrantOrphanTickStrategy {
        id: i32,
        manager: Rc<RefCell<OrphanStrategyManager>>,
        tick_hits: Rc<Cell<u32>>,
        active: bool,
    }

    impl Strategy for ReentrantOrphanTickStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, _order_id: i64) -> bool {
            false
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {
            self.tick_hits.set(self.tick_hits.get() + 1);
            let _ = self.manager.borrow_mut().contains(self.id);
            self.active = false;
        }

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some("BTCUSDT")
        }
    }

    #[test]
    fn period_clock_driver_releases_strategy_manager_borrow_before_callback() {
        let manager = Rc::new(RefCell::new(StrategyManager::new()));
        let tick_hits = Rc::new(Cell::new(0));
        manager.borrow_mut().insert(Box::new(ReentrantTickStrategy {
            id: 101,
            manager: manager.clone(),
            tick_hits: tick_hits.clone(),
            active: true,
        }));

        let inspected = drive_strategy_manager_period_clock_rc(&manager, 0);

        assert_eq!(inspected, 1);
        assert_eq!(tick_hits.get(), 1);
        assert!(!manager.borrow().contains(101));
    }

    #[test]
    fn period_clock_driver_releases_orphan_manager_borrow_before_callback() {
        let manager = Rc::new(RefCell::new(OrphanStrategyManager::new()));
        let tick_hits = Rc::new(Cell::new(0));
        manager
            .borrow_mut()
            .insert(Box::new(ReentrantOrphanTickStrategy {
                id: 202,
                manager: manager.clone(),
                tick_hits: tick_hits.clone(),
                active: true,
            }));

        let inspected = drive_orphan_manager_period_clock_rc(&manager, 0);

        assert_eq!(inspected, 1);
        assert_eq!(tick_hits.get(), 1);
        assert!(!manager.borrow().contains(202));
    }

    #[test]
    fn period_clock_driver_removes_empty_orphan_strategy() {
        let manager = Rc::new(RefCell::new(OrphanStrategyManager::new()));
        manager
            .borrow_mut()
            .insert(Box::new(OrphanOrderStrategy::new(303, "BTCUSDT")));

        let inspected = drive_orphan_manager_period_clock_rc(&manager, 0);

        assert_eq!(inspected, 1);
        assert!(!manager.borrow().contains(303));
        assert!(manager.borrow().is_empty());
    }
}
