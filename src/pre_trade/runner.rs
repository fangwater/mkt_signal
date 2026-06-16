use crate::pre_trade::account_open_block::drive_account_open_block_capacity_poll;
use crate::pre_trade::auto_collection_service::AutoCollectionService;
use crate::pre_trade::auto_repay_service::AutoRepayService;
use crate::pre_trade::intra_bwd_symbol_list::IntraBwdSymbolList;
use crate::pre_trade::leverage_guard::LeverageGuard;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateLimiter;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::query_eng_channel::QueryEngHub;
use crate::pre_trade::reactor_latency::{record_stage_latency, ReactorStage};
use crate::pre_trade::resample_channel::ResampleChannel;
use crate::pre_trade::runtime_flags::{enable_ipc_fast_poll, suppress_pre_submit_hot_path_logs};
use crate::pre_trade::signal_channel::{OpenSignalDropReason, SignalChannel};
use crate::pre_trade::signal_throttle::log_active_signal_throttles;
use crate::pre_trade::taker_decision_model::PreTradeTakerDecisionModel;
use crate::pre_trade::trade_eng_channel::TradeEngHub;
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
use trade_engine::query_request::{GenericQueryRequest, QueryRequestType};

const PARAM_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const INTRA_BWD_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const LEVERAGE_GUARD_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const SNAPSHOT_QUERY_INTERVAL: Duration = Duration::from_secs(60);
const EXPOSURE_TABLE_PRINT_INTERVAL: Duration = Duration::from_secs(10);

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
            signal: fast_poll_budget("PRE_TRADE_FAST_SIGNAL_BUDGET", 16),
            trade_resp: fast_poll_budget("PRE_TRADE_FAST_TRADE_RESP_BUDGET", 16),
            monitor_state: fast_poll_budget("PRE_TRADE_FAST_MONITOR_STATE_BUDGET", 16),
            query_resp: fast_poll_budget("PRE_TRADE_FAST_QUERY_RESP_BUDGET", 16),
            model_update: fast_poll_budget("PRE_TRADE_FAST_MODEL_UPDATE_BUDGET", 16),
            period_strategy: fast_poll_budget("PRE_TRADE_FAST_PERIOD_STRATEGY_BUDGET", 16),
            period_orphan: fast_poll_budget("PRE_TRADE_FAST_PERIOD_ORPHAN_BUDGET", 16),
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
    key_suffix: String,
}

impl IntraBwdRefreshConfig {
    pub fn new(redis: RedisSettings, key_suffix: String) -> Self {
        Self { redis, key_suffix }
    }
}

#[derive(Clone)]
pub struct SnapshotQueryConfig {
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
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
        }
    }
}

pub struct PreTrade {
    param_refresh: Option<ParamRefreshConfig>,
    intra_bwd_refresh: Option<IntraBwdRefreshConfig>,
    snapshot_query: Option<SnapshotQueryConfig>,
    auto_repay: Option<AutoRepayService>,
    auto_collection: Option<AutoCollectionService>,
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
    let need_okex = open_venue.trade_engine_exchange() == "okex"
        || hedge_venue.trade_engine_exchange() == "okex";
    let need_gate = open_venue.trade_engine_exchange() == "gate"
        || hedge_venue.trade_engine_exchange() == "gate";
    let need_bybit = open_venue.trade_engine_exchange() == "bybit"
        || hedge_venue.trade_engine_exchange() == "bybit";
    let need_bitget = open_venue.trade_engine_exchange() == "bitget"
        || hedge_venue.trade_engine_exchange() == "bitget";

    if !need_binance && !need_okex && !need_gate && !need_bybit && !need_bitget {
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
            publish(
                "binance",
                QueryRequestType::BinanceSpotAccountSnapshotStd,
                Bytes::new(),
                "binance spot account snapshot (standard)",
            );
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
        } else {
            publish(
                "binance",
                QueryRequestType::BinancePmBalanceSnapshot,
                Bytes::new(),
                "binance PM balance snapshot",
            );
            publish(
                "binance",
                QueryRequestType::BinanceUmAccountSnapshot,
                Bytes::new(),
                "binance UM account snapshot",
            );
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
        publish(
            "bitget",
            QueryRequestType::BitgetPositionsSnapshot,
            Bytes::from_static(b"category=USDT-FUTURES"),
            "bitget UTA current positions snapshot",
        );
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
            snapshot_query: None,
            auto_repay: None,
            auto_collection: None,
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

    pub fn with_snapshot_query(mut self, config: SnapshotQueryConfig) -> Self {
        self.snapshot_query = Some(config);
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

    pub async fn run(self) -> Result<()> {
        info!("pre_trade main loop starting");
        let param_refresh = self.param_refresh;
        let intra_bwd_refresh = self.intra_bwd_refresh;
        let snapshot_query = self.snapshot_query;
        let mut auto_repay = self.auto_repay;
        let mut auto_collection = self.auto_collection;

        // 定时器状态
        let resample_interval = std::time::Duration::from_secs(3);
        let mut next_resample = std::time::Instant::now() + resample_interval;
        let mut next_exposure_table_print =
            std::time::Instant::now() + EXPOSURE_TABLE_PRINT_INTERVAL;
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
        let fast_poll = enable_ipc_fast_poll();
        let fast_poll_budgets = FastPollDispatchBudgets::from_env();
        if !fast_poll {
            if let Some(refresh_cfg) = param_refresh.as_ref() {
                PreTradeParamsLoader::start_background_refresh(
                    refresh_cfg.redis.clone(),
                    refresh_cfg.env_name.clone(),
                    refresh_cfg.open_venue,
                    refresh_cfg.hedge_venue,
                );
            }
            if let Some(refresh_cfg) = intra_bwd_refresh.as_ref() {
                IntraBwdSymbolList::start_background_refresh(
                    refresh_cfg.redis.clone(),
                    refresh_cfg.key_suffix.clone(),
                );
            }
            if let Some(auto_repay) = auto_repay.take() {
                auto_repay.start();
            }
            if let Some(auto_collection) = auto_collection.take() {
                auto_collection.start_startup_and_daily_task();
            }
        }
        let mut next_param_refresh = Instant::now() + PARAM_REFRESH_INTERVAL;
        let mut next_intra_bwd_refresh = Instant::now() + INTRA_BWD_REFRESH_INTERVAL;
        let mut next_leverage_guard_refresh = Instant::now() + LEVERAGE_GUARD_REFRESH_INTERVAL;
        let mut next_snapshot_query = Instant::now();
        let mut next_auto_repay = Instant::now();
        let mut next_auto_collection = Instant::now();
        let mut last_loop_end_us = get_timestamp_us();
        let mut pending_maintenance_open_drop_reason: Option<OpenSignalDropReason> = None;
        info!(
            "pre_trade signal throttle log started (interval={}s)",
            throttle_log_interval_secs
        );
        info!("pre_trade MM open order rate cleanup started (interval=10s window=60s)");
        info!(
            "pre_trade param refresh configured (enable_ipc_fast_poll={} blocking_refresh={} async_background_refresh={} interval_s={})",
            fast_poll,
            fast_poll && param_refresh.is_some(),
            !fast_poll && param_refresh.is_some(),
            PARAM_REFRESH_INTERVAL.as_secs()
        );

        // 周期检查频率设为 20ms，提高 MM trigger 响应及时性，同时保持较低调度开销。
        // IPC hot path 不等待这个 tick；空闲时先做 bounded busy-poll，超过预算才 yield。
        let period_clock_interval = Duration::from_millis(20);
        let mut next_period_clock = Instant::now();
        let mut pending_period_strategy_inspect = 0usize;
        let mut pending_period_orphan_inspect = 0usize;
        let idle_spin_iters = if fast_poll {
            std::env::var("PRE_TRADE_REACTOR_IDLE_SPIN_ITERS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(64)
        } else {
            0
        };
        let idle_sleep = Duration::from_micros(500);
        let mut idle_spin_count = 0usize;
        info!(
            "pre_trade reactor idle spin configured (enable_ipc_fast_poll={} iters={} idle_sleep_us={})",
            fast_poll,
            idle_spin_iters,
            idle_sleep.as_micros()
        );
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
            let open_drop_reason = pending_maintenance_open_drop_reason.take();
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

            let monitor_refresh_start_us = get_timestamp_us();
            let (monitor_has_work, basic_state_refreshed) = if fast_poll {
                MonitorChannel::drain_pending_state_updates_with_refresh_limit(
                    fast_poll_budgets.monitor_state,
                )
            } else {
                MonitorChannel::drain_pending_state_updates_with_refresh()
            };
            has_work |= monitor_has_work;
            if fast_poll && basic_state_refreshed {
                let refresh_elapsed_us =
                    get_timestamp_us().saturating_sub(monitor_refresh_start_us);
                next_loop_open_drop_reason = select_slower_open_drop_reason(
                    next_loop_open_drop_reason,
                    Some(OpenSignalDropReason {
                        source: "basic_state_refresh",
                        elapsed_us: refresh_elapsed_us,
                        threshold_us: 0,
                    }),
                );
            }
            if fast_poll && monitor_has_work {
                finish_fast_poll_work!(next_loop_open_drop_reason);
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
                if let Some(refresh_cfg) = param_refresh.as_ref() {
                    if instant_now >= next_param_refresh {
                        let refresh_start_us = get_timestamp_us();
                        let refresh_elapsed_us = match PreTradeParamsLoader::instance()
                            .load_from_redis_blocking(
                                &refresh_cfg.redis,
                                refresh_cfg.env_name.as_deref(),
                                refresh_cfg.open_venue,
                                refresh_cfg.hedge_venue,
                            ) {
                            Ok(()) => {
                                let elapsed_us =
                                    get_timestamp_us().saturating_sub(refresh_start_us);
                                info!(
                                    "pre_trade blocking risk params refresh ok elapsed_us={elapsed_us}"
                                );
                                elapsed_us
                            }
                            Err(err) => {
                                let elapsed_us =
                                    get_timestamp_us().saturating_sub(refresh_start_us);
                                warn!(
                                    "pre_trade blocking risk params refresh failed elapsed_us={} err={:#}",
                                    elapsed_us, err
                                );
                                elapsed_us
                            }
                        };
                        next_loop_open_drop_reason = select_slower_open_drop_reason(
                            next_loop_open_drop_reason,
                            Some(OpenSignalDropReason {
                                source: "param_refresh",
                                elapsed_us: refresh_elapsed_us,
                                threshold_us: 0,
                            }),
                        );
                        while instant_now >= next_param_refresh {
                            next_param_refresh += PARAM_REFRESH_INTERVAL;
                        }
                        finish_fast_poll_work!(next_loop_open_drop_reason);
                    }
                }
                if let Some(refresh_cfg) = intra_bwd_refresh.as_ref() {
                    if instant_now >= next_intra_bwd_refresh {
                        let refresh_start_us = get_timestamp_us();
                        let refresh_elapsed_us = match IntraBwdSymbolList::load_from_redis_blocking(
                            &refresh_cfg.redis,
                            &refresh_cfg.key_suffix,
                        ) {
                            Ok(()) => {
                                let elapsed_us =
                                    get_timestamp_us().saturating_sub(refresh_start_us);
                                info!(
                                    "pre_trade blocking intra_bwd refresh ok elapsed_us={elapsed_us}"
                                );
                                elapsed_us
                            }
                            Err(err) => {
                                let elapsed_us =
                                    get_timestamp_us().saturating_sub(refresh_start_us);
                                warn!(
                                    "pre_trade blocking intra_bwd refresh failed elapsed_us={} err={:#}",
                                    elapsed_us, err
                                );
                                elapsed_us
                            }
                        };
                        next_loop_open_drop_reason = select_slower_open_drop_reason(
                            next_loop_open_drop_reason,
                            Some(OpenSignalDropReason {
                                source: "intra_bwd_refresh",
                                elapsed_us: refresh_elapsed_us,
                                threshold_us: 0,
                            }),
                        );
                        while instant_now >= next_intra_bwd_refresh {
                            next_intra_bwd_refresh += INTRA_BWD_REFRESH_INTERVAL;
                        }
                        finish_fast_poll_work!(next_loop_open_drop_reason);
                    }
                }
                let scheduled_leverage_refresh = instant_now >= next_leverage_guard_refresh;
                let requested_leverage_refresh = LeverageGuard::take_fast_poll_refresh_request();
                let leverage_refresh_source = requested_leverage_refresh
                    .or_else(|| scheduled_leverage_refresh.then_some("background_interval"));
                if let Some(leverage_refresh_source) = leverage_refresh_source {
                    let refresh_start_us = get_timestamp_us();
                    let refresh_elapsed_us = match LeverageGuard::refresh_blocking_for_fast_poll(
                        leverage_refresh_source,
                    ) {
                        Ok(true) => {
                            let elapsed_us = get_timestamp_us().saturating_sub(refresh_start_us);
                            info!(
                                "pre_trade blocking leverage guard refresh ok elapsed_us={elapsed_us}"
                            );
                            Some(elapsed_us)
                        }
                        Ok(false) => None,
                        Err(err) => {
                            let elapsed_us = get_timestamp_us().saturating_sub(refresh_start_us);
                            warn!(
                                "pre_trade blocking leverage guard refresh failed elapsed_us={} err={:#}",
                                elapsed_us, err
                            );
                            Some(elapsed_us)
                        }
                    };
                    if let Some(refresh_elapsed_us) = refresh_elapsed_us {
                        next_loop_open_drop_reason = select_slower_open_drop_reason(
                            next_loop_open_drop_reason,
                            Some(OpenSignalDropReason {
                                source: leverage_refresh_source,
                                elapsed_us: refresh_elapsed_us,
                                threshold_us: 0,
                            }),
                        );
                    }
                    if scheduled_leverage_refresh {
                        while instant_now >= next_leverage_guard_refresh {
                            next_leverage_guard_refresh += LEVERAGE_GUARD_REFRESH_INTERVAL;
                        }
                    }
                    finish_fast_poll_work!(next_loop_open_drop_reason);
                }
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
                if let Some(auto_repay) = auto_repay.as_ref() {
                    if instant_now >= next_auto_repay {
                        let repay_start_us = get_timestamp_us();
                        auto_repay.run_once("fast_poll_tick").await;
                        let repay_elapsed_us = get_timestamp_us().saturating_sub(repay_start_us);
                        info!("pre_trade auto-repay completed elapsed_us={repay_elapsed_us}");
                        next_loop_open_drop_reason = select_slower_open_drop_reason(
                            next_loop_open_drop_reason,
                            Some(OpenSignalDropReason {
                                source: "auto_repay",
                                elapsed_us: repay_elapsed_us,
                                threshold_us: 0,
                            }),
                        );
                        next_auto_repay =
                            Instant::now() + AutoRepayService::time_until_next_55min();
                        finish_fast_poll_work!(next_loop_open_drop_reason);
                    }
                }
                if let Some(auto_collection) = auto_collection.as_ref() {
                    if instant_now >= next_auto_collection {
                        let collection_start_us = get_timestamp_us();
                        auto_collection.run_once("fast_poll_tick").await;
                        let collection_elapsed_us =
                            get_timestamp_us().saturating_sub(collection_start_us);
                        info!(
                            "pre_trade auto-collection completed elapsed_us={collection_elapsed_us}"
                        );
                        next_loop_open_drop_reason = select_slower_open_drop_reason(
                            next_loop_open_drop_reason,
                            Some(OpenSignalDropReason {
                                source: "auto_collection",
                                elapsed_us: collection_elapsed_us,
                                threshold_us: 0,
                            }),
                        );
                        next_auto_collection =
                            Instant::now() + AutoCollectionService::time_until_next_shanghai_noon();
                        finish_fast_poll_work!(next_loop_open_drop_reason);
                    }
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
                has_work |= SignalChannel::drain_pending();
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

                if instant_now >= next_resample {
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

                if instant_now >= next_exposure_table_print {
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
                while instant_now >= next_resample {
                    let result = ResampleChannel::with(|ch| ch.publish_resample_entries());
                    if let Err(err) = result {
                        warn!("pre_trade resample publish failed: {err:#}");
                        next_resample = Instant::now() + resample_interval;
                        break;
                    }
                    next_resample += resample_interval;
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

            if idle_spin_count < idle_spin_iters {
                idle_spin_count += 1;
                std::hint::spin_loop();
                if fast_poll {
                    pending_maintenance_open_drop_reason = next_loop_open_drop_reason;
                }
                last_loop_end_us = get_timestamp_us();
                continue;
            }
            idle_spin_count = 0;

            if fast_poll {
                pending_maintenance_open_drop_reason = next_loop_open_drop_reason;
            }
            last_loop_end_us = get_timestamp_us();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tokio::task::yield_now(), if fast_poll => {}
                _ = tokio::time::sleep(idle_sleep), if !fast_poll => {}
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{drive_orphan_manager_period_clock_rc, drive_strategy_manager_period_clock_rc};
    use crate::strategy::orphan_order_strategy::OrphanOrderStrategy;
    use crate::strategy::{OrphanStrategyManager, Strategy, StrategyManager};
    use order_common::{OrderUpdate, TradeUpdate};
    use signal_common::trade_signal::TradeSignal;
    use std::any::Any;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

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
