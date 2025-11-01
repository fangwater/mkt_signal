use crate::common::account_msg::{
    get_event_type as get_account_event_type, AccountEventType, AccountPositionMsg,
    AccountUpdateBalanceMsg, AccountUpdateFlushMsg, AccountUpdatePositionMsg, BalanceUpdateMsg,
    ExecutionReportMsg, OrderTradeUpdateMsg,
};
use crate::common::iceoryx_publisher::{
    ResamplePublisher, SignalPublisher, RESAMPLE_PAYLOAD, SIGNAL_PAYLOAD,
};
use crate::common::min_qty_table::MinQtyTable;
use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::binance_pm_spot_manager::{BinancePmSpotAccountManager, BinanceSpotBalance};
use crate::pre_trade::binance_pm_um_manager::{
    BinancePmUmAccountManager, BinanceUmAccountSnapshot, BinanceUmPosition,
};
use crate::pre_trade::config::{
    AccountStreamCfg, PreTradeCfg, SignalSubscriptionsCfg, StrategyParamsCfg, TradeEngineRespCfg,
};
use crate::pre_trade::event::AccountEvent;
use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use crate::signal::binance_forward_arb::{
    BinSingleForwardArbCloseMarginCtx, BinSingleForwardArbCloseUmCtx, BinSingleForwardArbHedgeCtx,
    BinSingleForwardArbLadderCancelCtx, BinSingleForwardArbOpenCtx, BinSingleForwardArbStrategy,
};
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::signal::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradePositionResampleEntry,
    PreTradeRiskResampleEntry, PreTradeSpotBalanceRow, PreTradeUmPositionRow,
    PRE_TRADE_EXPOSURE_CHANNEL, PRE_TRADE_POSITIONS_CHANNEL, PRE_TRADE_RISK_CHANNEL,
};
use crate::signal::strategy::{Strategy, StrategyManager};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, error, info, warn};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

const ACCOUNT_PAYLOAD: usize = 16_384;
const TRADE_RESP_PAYLOAD: usize = 16_384;
const ORDER_REQ_PAYLOAD: usize = 4_096;

const NODE_PRE_TRADE_ACCOUNT: &str = "pre_trade_account";
const NODE_PRE_TRADE_TRADE_RESP: &str = "pre_trade_trade_resp";
const NODE_PRE_TRADE_SIGNAL_PREFIX: &str = "signals";
const NODE_PRE_TRADE_ORDER_REQ: &str = "pre_trade_order_req";
const NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";
const DERIVATIVES_SERVICE: &str = "data_pubs/binance-futures/derivatives";
const DERIVATIVES_PAYLOAD: usize = 128;

pub struct PreTrade {
    cfg: PreTradeCfg,
}

impl PreTrade {
    pub fn new(cfg: PreTradeCfg) -> Self {
        Self { cfg }
    }

    pub async fn run(self) -> Result<()> {
        info!("pre_trade starting");

        let bootstrap = BootstrapResources::load(&self.cfg).await?;

        let order_publisher = OrderPublisher::new(&bootstrap.order_req_service)?;
        let (order_tx, order_rx) = mpsc::unbounded_channel::<Bytes>();
        let (signal_tx, signal_rx) = mpsc::unbounded_channel::<Bytes>();

        // 初始化策略参数从 Redis
        let strategy_params = StrategyParamsCfg::default();

        let make_pub = |channel: &str| match ResamplePublisher::new(channel) {
            Ok(p) => Some(p),
            Err(err) => {
                warn!(
                    "failed to create pre_trade resample publisher on {}: {err:#}",
                    channel
                );
                None
            }
        };

        let resample_positions_pub = make_pub(PRE_TRADE_POSITIONS_CHANNEL);
        let resample_exposure_pub = make_pub(PRE_TRADE_EXPOSURE_CHANNEL);
        let resample_risk_pub = make_pub(PRE_TRADE_RISK_CHANNEL);
        let signal_record_pub = match SignalPublisher::new(PRE_TRADE_SIGNAL_RECORD_CHANNEL) {
            Ok(p) => Some(p),
            Err(err) => {
                warn!(
                    "failed to create pre_trade signal record publisher on {}: {err:#}",
                    PRE_TRADE_SIGNAL_RECORD_CHANNEL
                );
                None
            }
        };

        let mut runtime = RuntimeContext::new(
            bootstrap,
            order_tx.clone(),
            signal_tx.clone(),
            order_publisher,
            strategy_params,
            resample_positions_pub,
            resample_exposure_pub,
            resample_risk_pub,
            signal_record_pub,
        );

        // 首次从 Redis 拉取 pre-trade 参数
        if let Err(err) = runtime.reload_params().await {
            warn!("pre_trade initial params load failed: {err:#}");
        }

        let mut order_rx = order_rx;
        let mut internal_signal_rx = signal_rx;

        let mut account_rx = spawn_account_listener(&self.cfg.account_stream)?;
        let mut trade_resp_rx = spawn_trade_response_listener(&self.cfg.trade_engine)?;
        let mut external_signal_rx = spawn_signal_listeners(&self.cfg.signals)?;

        spawn_derivatives_worker(runtime.price_table.clone())?;

        // 提升周期检查频率到 100ms，使策略状态响应更及时
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    runtime.shutdown_tasks().await;
                    break;
                }
                Some(evt) = account_rx.recv() => {
                    if let Err(err) = handle_account_event(&mut runtime, evt) {
                        warn!("handle account event failed: {err:?}");
                    }
                }
                Some(resp) = trade_resp_rx.recv() => {
                    handle_trade_engine_response(&mut runtime, resp);
                }
                Some(signal) = external_signal_rx.recv() => {
                    handle_trade_signal(&mut runtime, signal);
                }
                Some(signal_bytes) = internal_signal_rx.recv() => {
                    match TradeSignal::from_bytes(&signal_bytes) {
                        Ok(signal) => handle_trade_signal(&mut runtime, signal),
                        Err(err) => warn!("failed to decode internal signal: {err}"),
                    }
                }
                Some(bytes) = order_rx.recv() => {
                    if let Err(err) = runtime.order_publisher.publish(&bytes) {
                        warn!("failed to publish order request: {err}");
                    }
                }
                _ = ticker.tick() => {
                    runtime.tick().await;
                }
                else => break,
            }
        }

        info!("pre_trade exiting");
        Ok(())
    }
}

struct BootstrapResources {
    //币安 合约资产管理器，基于统一账户update更新
    um_manager: BinancePmUmAccountManager,
    //币安 现货资产管理器, 基于统一账户update更新
    spot_manager: BinancePmSpotAccountManager,
    //币安 现货+合约敞口管理器
    exposure_manager: ExposureManager,
    //币安 标记价格表，辅助计算以usdt计价的资产敞口
    price_table: Rc<RefCell<PriceTable>>,
    // 交易对最小下单量/步进信息（spot/futures/margin）
    min_qty_table: Rc<MinQtyTable>,
    //收取订单请求的服务名称
    order_req_service: String,
}

impl BootstrapResources {
    async fn load(cfg: &PreTradeCfg) -> Result<Self> {
        let um_cfg = cfg
            .risk_checks
            .binance_pm_um
            .as_ref()
            .ok_or_else(|| anyhow!("risk_checks.binance_pm_um must be configured"))?;

        let um_api_key = std::env::var(&um_cfg.api_key_env)
            .map_err(|_| anyhow!("environment variable {} not set", um_cfg.api_key_env))?;
        let um_api_secret = std::env::var(&um_cfg.api_secret_env)
            .map_err(|_| anyhow!("environment variable {} not set", um_cfg.api_secret_env))?;

        let um_manager = BinancePmUmAccountManager::new(
            &um_cfg.rest_base,
            um_api_key.clone(),
            um_api_secret.clone(),
            um_cfg.recv_window_ms,
        );
        let um_snapshot = um_manager
            .init()
            .await
            .context("failed to load initial Binance UM snapshot")?;
        log_um_positions(&um_snapshot.positions);

        let spot_cfg = cfg
            .risk_checks
            .binance_spot
            .as_ref()
            .ok_or_else(|| anyhow!("risk_checks.binance_spot must be configured"))?;

        let spot_api_key = if spot_cfg.api_key_env == um_cfg.api_key_env {
            um_api_key.clone()
        } else {
            std::env::var(&spot_cfg.api_key_env)
                .map_err(|_| anyhow!("environment variable {} not set", spot_cfg.api_key_env))?
        };
        let spot_api_secret = if spot_cfg.api_secret_env == um_cfg.api_secret_env {
            um_api_secret.clone()
        } else {
            std::env::var(&spot_cfg.api_secret_env)
                .map_err(|_| anyhow!("environment variable {} not set", spot_cfg.api_secret_env))?
        };

        let asset_filter = spot_cfg
            .asset
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string());

        let spot_manager = BinancePmSpotAccountManager::new(
            &spot_cfg.rest_base,
            spot_api_key,
            spot_api_secret,
            spot_cfg.recv_window_ms,
            asset_filter,
        );
        let spot_snapshot = spot_manager
            .init()
            .await
            .context("failed to load initial Binance spot snapshot")?;
        log_spot_balances(&spot_snapshot.balances);

        let mut price_symbols: BTreeSet<String> = BTreeSet::new();
        collect_price_symbols(&mut price_symbols, &um_snapshot, &spot_snapshot);

        let price_table = Rc::new(RefCell::new(PriceTable::new()));
        {
            let mut table = price_table.borrow_mut();
            table
                .init(&price_symbols)
                .await
                .context("failed to load initial price table")?;
            log_price_table(&table.snapshot());
        }

        let mut exposure_manager = ExposureManager::new(&um_snapshot, &spot_snapshot);
        {
            let table = price_table.borrow();
            let snap = table.snapshot();
            // 基于初始价格对总权益/总敞口进行 USDT 计价
            exposure_manager.revalue_with_prices(&snap);
            log_exposures(exposure_manager.exposures(), &snap);
        }

        let order_req_service = resolve_order_req_service(&cfg.trade_engine);

        // 加载交易对 LOT_SIZE/PRICE_FILTER（spot/futures/margin），用于数量/价格对齐
        let mut min_qty_table = MinQtyTable::new();
        if let Err(err) = min_qty_table.refresh_binance().await {
            warn!("failed to refresh Binance exchange filters: {err:#}");
        }
        let min_qty_table = Rc::new(min_qty_table);

        Ok(Self {
            um_manager,
            spot_manager,
            exposure_manager,
            price_table,
            min_qty_table,
            order_req_service,
        })
    }
}

struct RuntimeContext {
    spot_manager: BinancePmSpotAccountManager,
    um_manager: BinancePmUmAccountManager,
    exposure_manager: Rc<RefCell<ExposureManager>>,
    price_table: Rc<RefCell<PriceTable>>,
    order_manager: Rc<RefCell<crate::pre_trade::order_manager::OrderManager>>,
    strategy_mgr: StrategyManager,
    order_tx: UnboundedSender<Bytes>,
    signal_tx: UnboundedSender<Bytes>,
    order_publisher: OrderPublisher,
    strategy_params: StrategyParamsCfg,
    max_pending_limit_orders: Rc<Cell<i32>>,
    min_qty_table: Rc<MinQtyTable>,
    dedup: crate::pre_trade::dedup::DedupCache,
    resample_positions_pub: Option<ResamplePublisher>,
    resample_exposure_pub: Option<ResamplePublisher>,
    resample_risk_pub: Option<ResamplePublisher>,
    signal_record_pub: Option<SignalPublisher>,
    resample_interval: std::time::Duration,
    next_resample: std::time::Instant,
    next_params_refresh: std::time::Instant,
    params_refresh_secs: u64,
    last_params_snapshot: Option<PreTradeParamsSnap>,
    strategy_activity: bool,
    ladder_cancel_activity: bool,
}

impl RuntimeContext {
    fn new(
        bootstrap: BootstrapResources,
        order_tx: UnboundedSender<Bytes>,
        signal_tx: UnboundedSender<Bytes>,
        order_publisher: OrderPublisher,
        strategy_params: StrategyParamsCfg,
        resample_positions_pub: Option<ResamplePublisher>,
        resample_exposure_pub: Option<ResamplePublisher>,
        resample_risk_pub: Option<ResamplePublisher>,
        signal_record_pub: Option<SignalPublisher>,
    ) -> Self {
        let BootstrapResources {
            um_manager,
            spot_manager,
            exposure_manager,
            price_table,
            min_qty_table,
            order_req_service: _,
        } = bootstrap;

        let exposure_manager_rc = Rc::new(RefCell::new(exposure_manager));

        Self {
            spot_manager,
            um_manager,
            exposure_manager: exposure_manager_rc,
            price_table,
            order_manager: Rc::new(RefCell::new(
                crate::pre_trade::order_manager::OrderManager::new(),
            )),
            strategy_mgr: StrategyManager::new(),
            order_tx,
            signal_tx,
            order_publisher,
            strategy_params,
            max_pending_limit_orders: Rc::new(Cell::new(3)),
            min_qty_table,
            dedup: crate::pre_trade::dedup::DedupCache::new(8192),
            resample_positions_pub,
            resample_exposure_pub,
            resample_risk_pub,
            signal_record_pub,
            resample_interval: std::time::Duration::from_secs(3),
            next_resample: std::time::Instant::now() + std::time::Duration::from_secs(3),
            next_params_refresh: std::time::Instant::now(),
            params_refresh_secs: 30,
            last_params_snapshot: None,
            strategy_activity: false,
            ladder_cancel_activity: false,
        }
    }

    async fn shutdown_tasks(&mut self) {
        // no-op: persistent store removed
    }

    fn cleanup_inactive(&mut self) {}

    fn insert_strategy(&mut self, symbol: String, strategy: Box<dyn Strategy>) {
        let strategy_id = strategy.get_id();
        self.strategy_mgr.insert(strategy);
        self.strategy_activity = true;
        debug!(
            "strategy inserted: strategy_id={} symbol={} active_total={}",
            strategy_id,
            symbol,
            self.strategy_mgr.len()
        );
    }

    fn publish_signal_record(&self, record: &SignalRecordMessage) {
        let Some(publisher) = &self.signal_record_pub else {
            return;
        };
        let payload = record.to_bytes();
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish signal record strategy_id={}: {err:#}",
                record.strategy_id
            );
        }
    }

    fn remove_strategy(&mut self, strategy_id: i32) {
        self.strategy_mgr.remove(strategy_id);
        self.strategy_activity = true;
    }

    fn with_strategy_mut<F>(&mut self, strategy_id: i32, mut f: F)
    where
        F: FnMut(&mut dyn Strategy),
    {
        if let Some(mut strategy) = self.strategy_mgr.take(strategy_id) {
            f(strategy.as_mut());
            if strategy.is_active() {
                self.strategy_mgr.insert(strategy);
            } else {
                drop(strategy);
                self.remove_strategy(strategy_id);
            }
        }
    }

    fn order_sender(&self) -> UnboundedSender<Bytes> {
        self.order_tx.clone()
    }

    fn signal_sender(&self) -> UnboundedSender<Bytes> {
        self.signal_tx.clone()
    }

    fn refresh_exposures(&mut self) {
        let Some(spot_snapshot) = self.spot_manager.snapshot() else {
            return;
        };
        let Some(um_snapshot) = self.um_manager.snapshot() else {
            return;
        };
        let positions_changed = self
            .exposure_manager
            .borrow_mut()
            .recompute(&um_snapshot, &spot_snapshot);

        // 结合最新标记价格，估值并打印三线表（USDT 计价的敞口），便于核对
        if let Some(price_snap) = self
            .price_table
            .try_borrow()
            .ok()
            .map(|table| table.snapshot())
        {
            {
                let mut mgr = self.exposure_manager.borrow_mut();
                mgr.revalue_with_prices(&price_snap);
            }
            if positions_changed {
                let exposures = self.exposure_manager.borrow();
                log_exposures(exposures.exposures(), &price_snap);
                log_exposure_summary(
                    exposures.total_equity(),
                    exposures.total_abs_exposure(),
                    exposures.total_position(),
                    self.strategy_params.max_leverage,
                );
            }
        }
    }

    async fn tick(&mut self) {
        let tick_start = std::time::Instant::now();
        let now = get_timestamp_us();
        let active_before = self.strategy_mgr.len();
        let inspected = self.strategy_mgr.handle_period_clock(now);
        self.cleanup_inactive();
        let active_after = self.strategy_mgr.len();
        if active_before != active_after {
            self.strategy_activity = true;
        }
        let instant_now = std::time::Instant::now();
        let mut params_refreshed = false;
        if instant_now >= self.next_params_refresh {
            match self.reload_params().await {
                Ok(()) => {
                    params_refreshed = true;
                }
                Err(err) => {
                    warn!("pre_trade params refresh failed: {err:#}");
                }
            }
            self.next_params_refresh =
                instant_now + std::time::Duration::from_secs(self.params_refresh_secs.max(5));
        }

        let mut resample_published = 0usize;
        if self.resample_positions_pub.is_some()
            || self.resample_exposure_pub.is_some()
            || self.resample_risk_pub.is_some()
        {
            while instant_now >= self.next_resample {
                match self.publish_resample_entries() {
                    Ok(count) => {
                        resample_published += count;
                    }
                    Err(err) => {
                        warn!("pre_trade resample publish failed: {err:#}");
                        self.next_resample = std::time::Instant::now() + self.resample_interval;
                        break;
                    }
                }
                self.next_resample += self.resample_interval;
            }
        }

        let elapsed_ms = tick_start.elapsed().as_secs_f64() * 1000.0;
        let strategy_activity =
            self.strategy_activity && (inspected > 0 || active_before != active_after);
        let ladder_activity = self.ladder_cancel_activity;
        let activity_detected = strategy_activity || ladder_activity;
        self.strategy_activity = false;
        self.ladder_cancel_activity = false;
        if activity_detected {
            debug!(
                "pre_trade period tick: inspected={} active_before={} active_after={} params_refreshed={} resample_entries={} elapsed_ms={:.2} strategy_activity={} ladder_cancel_activity={}",
                inspected,
                active_before,
                active_after,
                params_refreshed,
                resample_published,
                elapsed_ms,
                strategy_activity,
                ladder_activity
            );
        }
    }
}

impl RuntimeContext {
    async fn reload_params(&mut self) -> Result<()> {
        let url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
        let cli = redis::Client::open(url.clone())?;
        let mut mgr = redis::aio::ConnectionManager::new(cli).await?;
        let params: std::collections::HashMap<String, String> =
            redis::AsyncCommands::hgetall(&mut mgr, "binance_forward_arb_params").await?;
        let parse_f64 =
            |k: &str| -> Option<f64> { params.get(k).and_then(|v| v.parse::<f64>().ok()) };
        let parse_u64 =
            |k: &str| -> Option<u64> { params.get(k).and_then(|v| v.parse::<u64>().ok()) };
        let parse_i64 =
            |k: &str| -> Option<i64> { params.get(k).and_then(|v| v.parse::<i64>().ok()) };
        let mut new_refresh = self.params_refresh_secs;
        if let Some(v) = parse_u64("pre_trade_refresh_secs") {
            new_refresh = v;
        }
        let mut sp = self.strategy_params.clone();
        if let Some(v) = parse_f64("pre_trade_max_pos_u") {
            sp.max_pos_u = v;
        }
        if let Some(v) = parse_f64("pre_trade_max_symbol_exposure_ratio") {
            sp.max_symbol_exposure_ratio = v;
        }
        if let Some(v) = parse_f64("pre_trade_max_total_exposure_ratio") {
            sp.max_total_exposure_ratio = v;
        }
        if let Some(v) = parse_f64("pre_trade_max_leverage") {
            if v > 0.0 {
                sp.max_leverage = v;
            } else {
                warn!("pre_trade_max_leverage={} 无效，需大于 0，忽略更新", v);
            }
        }

        let mut new_pending_limit = self.max_pending_limit_orders.get();
        if let Some(v) = parse_i64("max_pending_limit_orders") {
            new_pending_limit = v.max(0) as i32;
        }

        if params.contains_key("pre_trade_store_enable")
            || params.contains_key("pre_trade_store_prefix")
            || params.contains_key("pre_trade_store_redis_url")
        {
            debug!("pre_trade store parameters detected but ignored (store feature removed)");
        }

        let pending_limit_changed = new_pending_limit != self.max_pending_limit_orders.get();
        if pending_limit_changed {
            self.max_pending_limit_orders.set(new_pending_limit);
            debug!(
                "max_pending_limit_orders updated to {}",
                self.max_pending_limit_orders.get()
            );
        }

        let snapshot = PreTradeParamsSnap {
            max_pos_u: sp.max_pos_u,
            max_symbol_exposure_ratio: sp.max_symbol_exposure_ratio,
            max_total_exposure_ratio: sp.max_total_exposure_ratio,
            max_leverage: sp.max_leverage,
            refresh_secs: new_refresh,
        };
        let changed = self
            .last_params_snapshot
            .as_ref()
            .map(|old| old != &snapshot)
            .unwrap_or(true);

        self.strategy_params = sp;
        self.params_refresh_secs = new_refresh;
        self.last_params_snapshot = Some(snapshot);
        if changed {
            debug!(
                "pre_trade params updated: max_pos_u={:.2} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} refresh={}s",
                self.strategy_params.max_pos_u,
                self.strategy_params.max_symbol_exposure_ratio,
                self.strategy_params.max_total_exposure_ratio,
                self.strategy_params.max_leverage,
                self.params_refresh_secs
            );
        }
        Ok(())
    }

    fn publish_resample_entries(&mut self) -> Result<usize> {
        if self.resample_positions_pub.is_none()
            && self.resample_exposure_pub.is_none()
            && self.resample_risk_pub.is_none()
        {
            return Ok(0);
        }

        let Some(spot_snapshot) = self.spot_manager.snapshot() else {
            return Ok(0);
        };
        let Some(um_snapshot) = self.um_manager.snapshot() else {
            return Ok(0);
        };

        let price_snapshot = self.price_table.borrow().snapshot();
        let ts_ms = (get_timestamp_us() / 1000) as i64;

        let mut exposures_mgr = self.exposure_manager.borrow_mut();
        exposures_mgr.revalue_with_prices(&price_snapshot);
        exposures_mgr.log_summary("resample估值");
        let exposures_vec = exposures_mgr.exposures().to_vec();
        let total_equity = exposures_mgr.total_equity();
        let total_abs_exposure = exposures_mgr.total_abs_exposure();
        let total_position = exposures_mgr.total_position();
        let spot_equity_usd = exposures_mgr.total_spot_value_usd();
        let borrowed_usd = exposures_mgr.total_borrowed_usd();
        let interest_usd = exposures_mgr.total_interest_usd();
        let um_unrealized_usd = exposures_mgr.total_um_unrealized();
        let max_leverage = self.strategy_params.max_leverage;
        drop(exposures_mgr);

        let mut published = 0usize;

        if let Some(publisher) = self.resample_positions_pub.as_ref() {
            let um_rows: Vec<PreTradeUmPositionRow> = um_snapshot
                .positions
                .iter()
                .map(|pos| PreTradeUmPositionRow {
                    symbol: pos.symbol.clone(),
                    side: pos.position_side.to_string(),
                    position_amount: pos.position_amt,
                    entry_price: pos.entry_price,
                    leverage: pos.leverage,
                    position_initial_margin: pos.position_initial_margin,
                    open_order_initial_margin: pos.open_order_initial_margin,
                    unrealized_profit: pos.unrealized_profit,
                })
                .collect();

            let spot_rows: Vec<PreTradeSpotBalanceRow> = spot_snapshot
                .balances
                .iter()
                .map(|bal| PreTradeSpotBalanceRow {
                    asset: bal.asset.clone(),
                    total_wallet: bal.total_wallet_balance,
                    cross_free: bal.cross_margin_free,
                    cross_locked: bal.cross_margin_locked,
                    cross_borrowed: bal.cross_margin_borrowed,
                    cross_interest: bal.cross_margin_interest,
                    um_wallet: bal.um_wallet_balance,
                    um_unrealized_pnl: bal.um_unrealized_pnl,
                })
                .collect();

            let entry = PreTradePositionResampleEntry {
                ts_ms,
                um_positions: um_rows,
                spot_balances: spot_rows,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        if let Some(publisher) = self.resample_exposure_pub.as_ref() {
            let mut rows: Vec<PreTradeExposureRow> = Vec::new();
            let mut exposure_sum_usdt = 0.0_f64;
            for entry in &exposures_vec {
                let asset_upper = entry.asset.to_uppercase();
                if asset_upper == "USDT" {
                    continue;
                }
                let symbol = format!("{}USDT", asset_upper);
                let mark = price_snapshot
                    .get(&symbol)
                    .map(|p| p.mark_price)
                    .unwrap_or(0.0);
                if mark == 0.0 && (entry.spot_total_wallet != 0.0 || entry.um_net_position != 0.0) {
                    debug!("missing mark price for {} when resampling exposure", symbol);
                }
                let spot_usdt = entry.spot_total_wallet * mark;
                let um_usdt = entry.um_net_position * mark;
                let exposure_usdt = spot_usdt + um_usdt;
                exposure_sum_usdt += exposure_usdt;
                rows.push(PreTradeExposureRow {
                    asset: entry.asset.clone(),
                    spot_qty: Some(entry.spot_total_wallet),
                    spot_usdt: Some(spot_usdt),
                    um_net_qty: Some(entry.um_net_position),
                    um_net_usdt: Some(um_usdt),
                    exposure_qty: Some(entry.exposure),
                    exposure_usdt: Some(exposure_usdt),
                    is_total: false,
                });
            }
            if !rows.is_empty() {
                rows.push(PreTradeExposureRow {
                    asset: "TOTAL".to_string(),
                    spot_qty: None,
                    spot_usdt: None,
                    um_net_qty: None,
                    um_net_usdt: None,
                    exposure_qty: None,
                    exposure_usdt: Some(exposure_sum_usdt),
                    is_total: true,
                });
            }

            let entry = PreTradeExposureResampleEntry { ts_ms, rows };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        if let Some(publisher) = self.resample_risk_pub.as_ref() {
            let leverage = if total_equity.abs() <= f64::EPSILON {
                0.0
            } else {
                total_position / total_equity
            };
            let entry = PreTradeRiskResampleEntry {
                ts_ms,
                total_equity,
                total_exposure: total_abs_exposure,
                total_position,
                spot_equity_usd,
                borrowed_usd,
                interest_usd,
                um_unrealized_usd,
                leverage,
                max_leverage,
            };
            if Self::publish_encoded(entry.to_bytes()?, publisher)? {
                published += 1;
            }
        }

        Ok(published)
    }

    fn publish_encoded(bytes: Vec<u8>, publisher: &ResamplePublisher) -> Result<bool> {
        if bytes.is_empty() {
            return Ok(false);
        }
        let mut buf = Vec::with_capacity(bytes.len() + 4);
        let len = bytes.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&bytes);
        if buf.len() > RESAMPLE_PAYLOAD {
            warn!(
                "pre_trade重采样载荷过大 ({} 字节，阈值 {} 字节)，已跳过",
                buf.len(),
                RESAMPLE_PAYLOAD
            );
            return Ok(false);
        }
        publisher.publish(&buf)?;
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PreTradeParamsSnap {
    max_pos_u: f64,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    refresh_secs: u64,
}

struct OrderPublisher {
    _node: Node<ipc::Service>,
    publisher: Publisher<ipc::Service, [u8; ORDER_REQ_PAYLOAD], ()>,
}

impl OrderPublisher {
    fn new(service: &str) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(NODE_PRE_TRADE_ORDER_REQ)?)
            .create::<ipc::Service>()?;
        let service = node
            .service_builder(&ServiceName::new(service)?)
            .publish_subscribe::<[u8; ORDER_REQ_PAYLOAD]>()
            .subscriber_max_buffer_size(256)
            .open_or_create()?;
        let publisher = service.publisher_builder().create()?;
        info!("order publisher ready: service={}", service.name());
        Ok(Self {
            _node: node,
            publisher,
        })
    }

    fn publish(&self, bytes: &Bytes) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > ORDER_REQ_PAYLOAD {
            warn!(
                "order request truncated: len={} capacity={}",
                bytes.len(),
                ORDER_REQ_PAYLOAD
            );
        }
        let mut buf = [0u8; ORDER_REQ_PAYLOAD];
        let copy_len = bytes.len().min(ORDER_REQ_PAYLOAD);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
        let sample = self.publisher.loan_uninit()?;
        let sample = sample.write_payload(buf);
        sample.send()?;
        Ok(())
    }
}

fn collect_price_symbols(
    set: &mut BTreeSet<String>,
    um_snapshot: &BinanceUmAccountSnapshot,
    spot_snapshot: &crate::pre_trade::binance_pm_spot_manager::BinanceSpotBalanceSnapshot,
) {
    for pos in &um_snapshot.positions {
        set.insert(pos.symbol.to_uppercase());
    }
    for bal in &spot_snapshot.balances {
        if bal.asset.eq_ignore_ascii_case("USDT") {
            continue;
        }
        set.insert(format!("{}USDT", bal.asset.to_uppercase()));
    }
}

fn resolve_order_req_service(cfg: &TradeEngineRespCfg) -> String {
    if let Some(req_service) = cfg.req_service.clone() {
        return req_service;
    }
    if cfg.service.contains("order_resps/") {
        return cfg.service.replace("order_resps/", "order_reqs/");
    }
    cfg.service.replace("resps", "reqs")
}

fn spawn_account_listener(cfg: &AccountStreamCfg) -> Result<UnboundedReceiver<AccountEvent>> {
    let (tx, rx) = mpsc::unbounded_channel();
    let service_name = cfg.service.clone();
    let label = cfg.label.clone().unwrap_or_else(|| cfg.service.clone());

    tokio::task::spawn_local(async move {
        let node_name = NODE_PRE_TRADE_ACCOUNT.to_string();
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()> =
                service.subscriber_builder().create()?;

            info!(
                "account stream subscribed: service={} label={}",
                service_name, label
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        let received_at = get_timestamp_us();

                        // Account frames format: [type:4][len:4][data:len]
                        let (frame_len, event_type_str) = if payload.len() >= 8 {
                            let msg_type = get_account_event_type(payload);
                            let body_len = u32::from_le_bytes([
                                payload[4], payload[5], payload[6], payload[7],
                            ]) as usize;
                            let total = 8 + body_len;
                            let clamped = total.min(payload.len());
                            (clamped, format!("{:?}", msg_type))
                        } else {
                            (payload.len(), "<too_short>".to_string())
                        };

                        let mut buf = payload[..frame_len].to_vec();
                        // debug!(
                        //     "account evt received: service={}, type={}, frame_bytes={}, cap_bytes={}",
                        //     service_name,
                        //     event_type_str,
                        //     buf.len(),
                        //     payload.len()
                        // );
                        // 降低冗余 info 日志（如 AccountPosition），保留上面的 debug 行

                        let evt = AccountEvent {
                            service: service_name.clone(),
                            received_at,
                            payload_len: buf.len(),
                            payload: std::mem::take(&mut buf),
                            event_type: Some(event_type_str),
                            event_time_ms: None,
                        };
                        if tx.send(evt).is_err() {
                            break;
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("account stream receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = result.await {
            warn!("account listener exited: {err:?}");
        }
    });

    Ok(rx)
}

fn spawn_trade_response_listener(
    cfg: &TradeEngineRespCfg,
) -> Result<UnboundedReceiver<TradeExecOutcome>> {
    let (tx, rx) = mpsc::unbounded_channel();
    let service_name = cfg.service.clone();
    let label = cfg.label.clone().unwrap_or_else(|| cfg.service.clone());

    tokio::task::spawn_local(async move {
        let node_name = NODE_PRE_TRADE_TRADE_RESP.to_string();
        let result = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; TRADE_RESP_PAYLOAD]>()
                .subscriber_max_buffer_size(256)
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; TRADE_RESP_PAYLOAD], ()> =
                service.subscriber_builder().create()?;

            info!(
                "trade response subscribed: service={} label={}",
                service_name, label
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let raw = trim_payload(sample.payload());
                        if raw.is_empty() {
                            continue;
                        }
                        match TradeExecOutcome::parse(raw.as_ref()) {
                            Some(event) => {
                                if tx.send(event).is_err() {
                                    break;
                                }
                            }
                            None => warn!("failed to parse trade response payload"),
                        }
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("trade response receive error: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = result.await {
            warn!("trade response listener exited: {err:?}");
        }
    });

    Ok(rx)
}

fn spawn_signal_listeners(cfg: &SignalSubscriptionsCfg) -> Result<UnboundedReceiver<TradeSignal>> {
    let (tx, rx) = mpsc::unbounded_channel();
    if cfg.channels.is_empty() {
        info!("no signal channels configured");
        return Ok(rx);
    }

    for channel in &cfg.channels {
        let channel_name = channel.clone();
        let channel_label = channel.clone();
        // Real service path used by publisher/subscriber
        let service_path = format!("signal_pubs/{}", channel_name);
        let tx_clone = tx.clone();

        tokio::task::spawn_local(async move {
            let node_name = signal_node_name(&channel_name);
            let result = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_path)?)
                    .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
                    .max_publishers(1)
                    .max_subscribers(32)
                    .history_size(128)
                    .subscriber_max_buffer_size(256)
                    .open_or_create()?;
                let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!(
                    "signal subscribed: node={} service={} channel={}",
                    node_name,
                    service.name(),
                    channel_name
                );

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = trim_payload(sample.payload());
                            if payload.is_empty() {
                                continue;
                            }
                            match TradeSignal::from_bytes(&payload) {
                                Ok(signal) => {
                                    if tx_clone.send(signal).is_err() {
                                        break;
                                    }
                                }
                                Err(err) => warn!(
                                    "failed to decode trade signal from channel {}: {}",
                                    channel_name, err
                                ),
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
                            warn!("signal receive error (channel={}): {err}", channel_name);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            };

            if let Err(err) = result.await {
                warn!(
                    "signal listener exited (channel={}): {err:?}",
                    channel_label
                );
            }
        });
    }

    Ok(rx)
}

fn handle_account_event(ctx: &mut RuntimeContext, evt: AccountEvent) -> Result<()> {
    if evt.payload.len() < 8 {
        anyhow::bail!("account payload too short: {} bytes", evt.payload.len());
    }

    let msg_type = get_account_event_type(&evt.payload);
    let hdr_len_bytes = u32::from_le_bytes([
        evt.payload[4],
        evt.payload[5],
        evt.payload[6],
        evt.payload[7],
    ]) as usize;
    // debug!(
    //     "account msg header: service={}, decoded_type={:?}, declared_len={}, evt_meta=({:?},{:?}), first8={:02X?}",
    //     evt.service,
    //     msg_type,
    //     hdr_len_bytes,
    //     evt.event_type,
    //     evt.event_time_ms,
    //     &evt.payload[..8]
    // );
    let payload_len = hdr_len_bytes;

    if evt.payload.len() < 8 + payload_len {
        anyhow::bail!(
            "account payload truncated: have {} expect {}",
            evt.payload.len(),
            8 + payload_len
        );
    }

    let data = &evt.payload[8..8 + payload_len];

    match msg_type {
        AccountEventType::AccountPosition => {
            let msg = AccountPositionMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_account_position(&msg);
            if !ctx.dedup.insert_check(key) {
                // debug!(
                //     "dedup drop AccountPosition: asset={} update_id={} event_time={}",
                //     msg.asset, msg.update_id, msg.event_time
                // );
                return Ok(());
            }
            ctx.spot_manager.apply_account_position(
                &msg.asset,
                msg.free_balance,
                msg.locked_balance,
                msg.event_time,
            );
            ctx.refresh_exposures();
        }
        AccountEventType::BalanceUpdate => {
            let msg = BalanceUpdateMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_balance_update(&msg);
            if !ctx.dedup.insert_check(key) {
                // debug!(
                //     "dedup drop BalanceUpdate: asset={} update_id={} event_time={}",
                //     msg.asset, msg.update_id, msg.event_time
                // );
                return Ok(());
            }
            debug!(
                "balanceUpdate: asset={} delta={} event_time={} tx_time={} update_id={}",
                msg.asset, msg.delta, msg.event_time, msg.transaction_time, msg.update_id
            );
            ctx.spot_manager
                .apply_balance_delta(&msg.asset, msg.delta, msg.event_time);
            ctx.refresh_exposures();
        }
        AccountEventType::AccountUpdateBalance => {
            let msg = AccountUpdateBalanceMsg::from_bytes(data)?;
            debug!(
                "accountUpdateBalance: asset={} reason={} bu={} wallet={} cross_wallet={} change={} event_time={} tx_time={}",
                msg.asset,
                msg.reason,
                msg.business_unit,
                msg.wallet_balance,
                msg.cross_wallet_balance,
                msg.balance_change,
                msg.event_time,
                msg.transaction_time
            );
            if msg.business_unit.eq_ignore_ascii_case("UM") {
                ctx.spot_manager.apply_um_wallet_snapshot(
                    &msg.asset,
                    msg.wallet_balance,
                    msg.event_time,
                );
            } else {
                ctx.spot_manager.apply_balance_snapshot(
                    &msg.asset,
                    msg.wallet_balance,
                    msg.cross_wallet_balance,
                    msg.balance_change,
                    msg.event_time,
                );
            }
        }
        AccountEventType::AccountUpdatePosition => {
            let msg = AccountUpdatePositionMsg::from_bytes(data)?;
            debug!(
                "accountUpdatePosition: symbol={} side={} pos_amt={} entry_px={} acc_realized={} uPnL={} breakeven={} reason={} bu={} event_time={} tx_time={}",
                msg.symbol,
                msg.position_side,
                msg.position_amount,
                msg.entry_price,
                msg.accumulated_realized,
                msg.unrealized_pnl,
                msg.breakeven_price,
                msg.reason,
                msg.business_unit,
                msg.event_time,
                msg.transaction_time
            );
            ctx.um_manager.apply_position_update(
                &msg.symbol,
                msg.position_side,
                msg.position_amount,
                msg.entry_price,
                msg.unrealized_pnl,
                msg.breakeven_price,
                msg.event_time,
            );
        }
        AccountEventType::AccountUpdateFlush => {
            let msg = AccountUpdateFlushMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_account_update_flush(&msg);
            if !ctx.dedup.insert_check(key) {
                return Ok(());
            }
            debug!(
                "accountUpdateFlush: scope={} event_time={} hash={:#x}",
                msg.scope, msg.event_time, msg.hash
            );
            ctx.refresh_exposures();
        }
        AccountEventType::ExecutionReport => {
            let report = ExecutionReportMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_execution_report(&report);
            if !ctx.dedup.insert_check(key) {
                // debug!(
                //     "dedup drop ExecutionReport: symbol={} ord={} trade={} x={} X={}",
                //     report.symbol, report.order_id, report.trade_id, report.execution_type, report.order_status
                // );
                return Ok(());
            }
            debug!(
                "executionReport: sym={} cli_id={} ord_id={} status={}",
                report.symbol, report.client_order_id, report.order_id, report.order_status
            );
            dispatch_execution_report(ctx, &report);
        }
        AccountEventType::OrderTradeUpdate => {
            let update = OrderTradeUpdateMsg::from_bytes(data)?;
            let key = crate::pre_trade::dedup::key_order_trade_update(&update);
            if !ctx.dedup.insert_check(key) {
                // debug!(
                //     "dedup drop OrderTradeUpdate: symbol={} ord={} trade={} x={} X={}",
                //     update.symbol, update.order_id, update.trade_id, update.execution_type, update.order_status
                // );
                return Ok(());
            }
            debug!(
                "orderTradeUpdate: sym={}, cli_id={}, cli_str='{}', ord_id={}, trade_id={}, side={}, pos_side={}, maker={}, reduce={}, otype={}, tif={}, x={}, X={}, px={}, qty={}, avg_px={}, stop_px={}, last_px={}, last_qty={}, cum_qty={}, fee_amt={}, fee_ccy={}, buy_notional={}, sell_notional={}, realized_pnl={}, times(E/T)={}/{}",
                update.symbol,
                update.client_order_id,
                update.client_order_id_str,
                update.order_id,
                update.trade_id,
                update.side,
                update.position_side,
                update.is_maker,
                update.reduce_only,
                update.order_type,
                update.time_in_force,
                update.execution_type,
                update.order_status,
                update.price,
                update.quantity,
                update.average_price,
                update.stop_price,
                update.last_executed_price,
                update.last_executed_quantity,
                update.cumulative_filled_quantity,
                update.commission_amount,
                update.commission_asset,
                update.buy_notional,
                update.sell_notional,
                update.realized_profit,
                update.event_time,
                update.transaction_time,
            );
            dispatch_order_trade_update(ctx, &update);
        }
        _ => {}
    }

    Ok(())
}

fn handle_trade_engine_response(ctx: &mut RuntimeContext, outcome: TradeExecOutcome) {
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(outcome.client_order_id) {
                strategy.handle_trade_response(&outcome);
            }
        });
    }

    ctx.cleanup_inactive();
}

fn handle_trade_signal(ctx: &mut RuntimeContext, signal: TradeSignal) {
    let raw_signal = signal.to_bytes();
    let is_ladder_cancel = matches!(
        signal.signal_type,
        SignalType::BinSingleForwardArbLadderCancel
    );
    if !is_ladder_cancel {
        debug!(
            "trade signal received: type={:?} generation_time={} ctx_len={}",
            signal.signal_type,
            signal.generation_time,
            signal.context.len()
        );
    }
    match signal.signal_type {
        SignalType::BinSingleForwardArbOpen => {
            match BinSingleForwardArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(open_ctx) => {
                    let symbol = open_ctx.spot_symbol.to_uppercase();
                    debug!(
                        "decoded open ctx: spot_symbol={} amount={} price={:.8} exp_time_us={}",
                        open_ctx.spot_symbol, open_ctx.amount, open_ctx.price, open_ctx.exp_time
                    );
                    let open_ctx_payload = open_ctx.to_bytes();
                    let max_limit = ctx.max_pending_limit_orders.get();
                    if max_limit > 0 {
                        let current_limit = {
                            let manager = ctx.order_manager.borrow();
                            manager.get_symbol_pending_limit_order_count(&symbol)
                        };
                        if current_limit >= max_limit {
                            warn!(
                                "BinSingleForwardArbStrategy: symbol={} 当前限价挂单数={} 已达到上限 {}，忽略开仓信号",
                                symbol,
                                current_limit,
                                max_limit
                            );
                            return;
                        }
                    }

                    let strategy_id = StrategyManager::generate_strategy_id(1);
                    let order_tx = ctx.order_sender();
                    let signal_tx = ctx.signal_sender();
                    let now = get_timestamp_us();

                    let mut strategy = BinSingleForwardArbStrategy::new_open(
                        strategy_id,
                        now,
                        symbol.clone(),
                        ctx.order_manager.clone(),
                        ctx.exposure_manager.clone(),
                        order_tx,
                        ctx.strategy_params.max_symbol_exposure_ratio,
                        ctx.strategy_params.max_total_exposure_ratio,
                        ctx.strategy_params.max_pos_u,
                        ctx.strategy_params.max_leverage,
                        ctx.max_pending_limit_orders.clone(),
                        ctx.min_qty_table.clone(),
                        ctx.price_table.clone(),
                    );
                    strategy.set_signal_sender(signal_tx);

                    debug!(
                        "strategy init for open signal: strategy_id={} symbol={} qty={:.6} price={:.8} tick={:.8} type={:?}",
                        strategy_id,
                        open_ctx.spot_symbol,
                        open_ctx.amount,
                        open_ctx.price,
                        open_ctx.price_tick,
                        open_ctx.order_type
                    );

                    strategy.handle_trade_signal(&raw_signal);

                    if strategy.is_active() {
                        ctx.insert_strategy(symbol, Box::new(strategy));
                        let record = SignalRecordMessage::new(
                            strategy_id,
                            SignalType::BinSingleForwardArbOpen,
                            open_ctx_payload.to_vec(),
                        );
                        ctx.publish_signal_record(&record);
                    }
                }
                Err(err) => warn!("failed to decode open context: {err}"),
            }
        }
        SignalType::BinSingleForwardArbCloseMargin => {
            match BinSingleForwardArbCloseMarginCtx::from_bytes(signal.context.clone()) {
                Ok(close_ctx) => {
                    let symbol = close_ctx.spot_symbol.to_uppercase();
                    if ctx.strategy_mgr.has_symbol(&symbol) {
                        dispatch_signal_to_existing_strategy(ctx, signal, raw_signal);
                    } else {
                        let strategy_id = StrategyManager::generate_strategy_id(2);
                        let order_tx = ctx.order_sender();
                        let signal_tx = ctx.signal_sender();
                        let now = get_timestamp_us();

                        let mut strategy = BinSingleForwardArbStrategy::new_close(
                            strategy_id,
                            now,
                            symbol.clone(),
                            ctx.order_manager.clone(),
                            ctx.exposure_manager.clone(),
                            order_tx,
                            ctx.strategy_params.max_symbol_exposure_ratio,
                            ctx.strategy_params.max_total_exposure_ratio,
                            ctx.strategy_params.max_pos_u,
                            ctx.strategy_params.max_leverage,
                            ctx.max_pending_limit_orders.clone(),
                            ctx.min_qty_table.clone(),
                            ctx.price_table.clone(),
                        );
                        strategy.set_signal_sender(signal_tx);

                        debug!(
                            "strategy init for close signal: strategy_id={} symbol={} price={:.8} tick={:.8}",
                            strategy_id,
                            close_ctx.spot_symbol,
                            close_ctx.limit_price,
                            close_ctx.price_tick
                        );

                        if let Err(err) = strategy.close_margin_with_limit(&close_ctx) {
                            warn!(
                                "failed to create margin close order for strategy_id={}: {}",
                                strategy_id, err
                            );
                        }

                        if strategy.is_active() {
                            ctx.insert_strategy(symbol, Box::new(strategy));
                        }
                    }
                }
                Err(err) => warn!("failed to decode margin close context: {err}"),
            }
        }
        SignalType::BinSingleForwardArbHedge | SignalType::BinSingleForwardArbCloseUm => {
            debug!(
                "dispatch signal to existing strategies: type={:?} ctx_len={}",
                signal.signal_type,
                signal.context.len()
            );
            dispatch_signal_to_existing_strategy(ctx, signal, raw_signal);
        }
        SignalType::BinSingleForwardArbLadderCancel => {
            ctx.ladder_cancel_activity = true;
            dispatch_signal_to_existing_strategy(ctx, signal, raw_signal);
        }
    }

    ctx.cleanup_inactive();
}

fn dispatch_signal_to_existing_strategy(
    ctx: &mut RuntimeContext,
    signal: TradeSignal,
    raw_signal: Bytes,
) {
    let signal_type = signal.signal_type.clone();
    let is_ladder_cancel = matches!(
        signal_type.clone(),
        SignalType::BinSingleForwardArbLadderCancel
    );
    let mut target_strategy_id: Option<i32> = None;
    let mut ladder_cancel_ctx: Option<BinSingleForwardArbLadderCancelCtx> = None;
    let mut ladder_symbol: Option<String> = None;
    let maybe_symbol = match signal_type {
        SignalType::BinSingleForwardArbHedge => {
            match BinSingleForwardArbHedgeCtx::from_bytes(signal.context.clone()) {
                Ok(hedge_ctx) => {
                    target_strategy_id = Some(hedge_ctx.strategy_id);
                    None
                }
                Err(err) => {
                    warn!("failed to decode hedge context: {err}");
                    return;
                }
            }
        }
        SignalType::BinSingleForwardArbCloseMargin => {
            match BinSingleForwardArbCloseMarginCtx::from_bytes(signal.context.clone()) {
                Ok(close_ctx) => {
                    debug!(
                        "decoded margin close ctx: spot_symbol={} limit_price={:.8} tick={:.8} exp_time={}",
                        close_ctx.spot_symbol, close_ctx.limit_price, close_ctx.price_tick, close_ctx.exp_time
                    );
                    Some(close_ctx.spot_symbol.to_uppercase())
                }
                Err(err) => {
                    warn!("failed to decode margin close context: {err}");
                    return;
                }
            }
        }
        SignalType::BinSingleForwardArbCloseUm => {
            match BinSingleForwardArbCloseUmCtx::from_bytes(signal.context.clone()) {
                Ok(um_ctx) => {
                    debug!(
                        "decoded UM close ctx: um_symbol={} exp_time={}",
                        um_ctx.um_symbol, um_ctx.exp_time
                    );
                    Some(um_ctx.um_symbol.to_uppercase())
                }
                Err(err) => {
                    warn!("failed to decode UM close context: {err}");
                    return;
                }
            }
        }
        SignalType::BinSingleForwardArbLadderCancel => {
            match BinSingleForwardArbLadderCancelCtx::from_bytes(signal.context.clone()) {
                Ok(cancel_ctx) => {
                    let symbol = cancel_ctx.spot_symbol.to_uppercase();
                    ladder_symbol = Some(symbol.clone());
                    ladder_cancel_ctx = Some(cancel_ctx);
                    Some(symbol)
                }
                Err(err) => {
                    warn!("failed to decode ladder cancel context: {err}");
                    return;
                }
            }
        }
        _ => None,
    };

    let strategy_ids: Vec<i32> = if let Some(strategy_id) = target_strategy_id {
        vec![strategy_id]
    } else if let Some(symbol) = maybe_symbol {
        ctx.strategy_mgr
            .ids_for_symbol(&symbol)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    } else {
        ctx.strategy_mgr.iter_ids().cloned().collect()
    };

    if is_ladder_cancel {
        let Some(cancel_ctx) = ladder_cancel_ctx else {
            return;
        };
        let symbol = ladder_symbol.unwrap_or_else(|| cancel_ctx.spot_symbol.to_uppercase());

        if strategy_ids.is_empty() {
            debug!(
                "ladder cancel signal ignored: symbol={} no active strategies",
                symbol
            );
            return;
        }

        debug!(
            "dispatch ladder cancel signal: symbol={} strategies={} bidask_sr={:.6} threshold={:.6} ctx_len={} generation_time={}",
            symbol,
            strategy_ids.len(),
            cancel_ctx.bidask_sr,
            cancel_ctx.cancel_threshold,
            signal.context.len(),
            signal.generation_time,
        );
    } else if strategy_ids.is_empty() {
        debug!("no active strategy matched for this signal");
    }

    for strategy_id in strategy_ids {
        let signal_bytes = raw_signal.clone();
        ctx.with_strategy_mut(strategy_id, |strategy| {
            debug!(
                "forward signal to strategy_id={} raw_len={}",
                strategy.get_id(),
                signal_bytes.len()
            );
            strategy.handle_trade_signal(&signal_bytes);
        });
    }
}

fn dispatch_execution_report(ctx: &mut RuntimeContext, report: &ExecutionReportMsg) {
    let order_id = report.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                strategy.handle_binance_margin_order_update(report);
            }
        });
    }

    if !matched {
        let expected_strategy_id = (order_id >> 32) as i32;
        debug!(
            "executionReport unmatched: sym={} cli_id={} ord_id={} status={} expect_strategy={}",
            report.symbol,
            report.client_order_id,
            report.order_id,
            report.order_status,
            expected_strategy_id
        );
    }

    ctx.cleanup_inactive();
}

fn dispatch_order_trade_update(ctx: &mut RuntimeContext, update: &OrderTradeUpdateMsg) {
    let order_id = update.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                strategy.handle_binance_futures_order_update(update);
            }
        });
    }

    if !matched {
        debug!(
            "orderTradeUpdate not matched to any strategy: client_order_id={} client_order_id_str='{}'",
            order_id,
            update.client_order_id_str
        );
    }

    ctx.cleanup_inactive();
}

fn trim_payload(payload: &[u8]) -> Bytes {
    // 直接拷贝整个 payload，避免将结尾合法的 0 截断
    Bytes::copy_from_slice(payload)
}

// 删除了基于 JSON 的账户元数据提取逻辑，账户事件采用二进制帧头解析
fn signal_node_name(channel: &str) -> String {
    format!(
        "{}{}",
        NODE_PRE_TRADE_SIGNAL_PREFIX,
        sanitize_suffix(channel)
    )
}

fn sanitize_suffix(raw: &str) -> std::borrow::Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    std::borrow::Cow::Owned(sanitized)
}

fn is_valid_node_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn log_um_positions(positions: &[BinanceUmPosition]) {
    if positions.is_empty() {
        info!("UM 持仓为空");
        return;
    }

    let mut rows: Vec<Vec<String>> = positions
        .iter()
        .map(|pos| {
            vec![
                pos.symbol.clone(),
                pos.position_side.to_string(),
                fmt_decimal(pos.position_amt),
                fmt_decimal(pos.entry_price),
                fmt_decimal(pos.leverage),
                fmt_decimal(pos.position_initial_margin),
                fmt_decimal(pos.open_order_initial_margin),
                fmt_decimal(pos.unrealized_profit),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let table = render_three_line_table(
        &[
            "Symbol", "Side", "PosAmt", "EntryPx", "Lev", "PosIM", "OpenIM", "uPnL",
        ],
        &rows,
    );
    info!("UM 持仓概览\n{}", table);
}

fn log_spot_balances(balances: &[BinanceSpotBalance]) {
    if balances.is_empty() {
        warn!("现货资产列表为空");
        return;
    }

    let mut rows: Vec<Vec<String>> = balances
        .iter()
        .map(|bal| {
            vec![
                bal.asset.clone(),
                fmt_decimal(bal.total_wallet_balance),
                fmt_decimal(bal.cross_margin_free),
                fmt_decimal(bal.cross_margin_locked),
                fmt_decimal(bal.cross_margin_borrowed),
                fmt_decimal(bal.um_wallet_balance),
                fmt_decimal(bal.um_unrealized_pnl),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));

    let table = render_three_line_table(
        &[
            "Asset",
            "TotalWallet",
            "CrossFree",
            "CrossLocked",
            "CrossBorrowed",
            "UMWallet",
            "UMUPNL",
        ],
        &rows,
    );
    info!("现货资产概览\n{}", table);
}

fn log_exposures(entries: &[ExposureEntry], price_map: &BTreeMap<String, PriceEntry>) {
    if entries.is_empty() {
        info!("非 USDT 资产敞口为空");
        return;
    }

    let mut sum_exposure_usdt = 0.0_f64;

    let mut rows: Vec<Vec<String>> = Vec::new();
    for entry in entries {
        let asset = entry.asset.to_uppercase();
        if asset == "USDT" {
            continue;
        }
        let sym = format!("{}USDT", asset);
        let mark = price_map.get(&sym).map(|p| p.mark_price).unwrap_or(0.0);
        if mark == 0.0 && (entry.spot_total_wallet != 0.0 || entry.um_net_position != 0.0) {
            debug!("missing mark price for {}, exposure valued as 0", sym);
        }
        let spot_usdt = entry.spot_total_wallet * mark;
        let um_usdt = entry.um_net_position * mark;
        let exposure_qty = entry.exposure;
        let exposure_usdt = spot_usdt + um_usdt;

        sum_exposure_usdt += exposure_usdt; // 注意：非绝对值

        rows.push(vec![
            entry.asset.clone(),
            fmt_decimal(entry.spot_total_wallet),
            fmt_decimal(spot_usdt),
            fmt_decimal(entry.um_net_position),
            fmt_decimal(um_usdt),
            fmt_decimal(exposure_qty),
            fmt_decimal(exposure_usdt),
        ]);
    }

    // 增加 TOTAL 行：仅展示 ExposureUSDT 的带符号汇总，其余列使用 "-" 占位。
    rows.push(vec![
        "TOTAL".to_string(),
        "-".to_string(),                // SpotQty
        "-".to_string(),                // SpotUSDT (不汇总显示)
        "-".to_string(),                // UMNetQty
        "-".to_string(),                // UMNetUSDT (不汇总显示)
        "-".to_string(),                // ExposureQty（不汇总，用户要求用 "-" 替代）
        fmt_decimal(sum_exposure_usdt), // ExposureUSDT (sum, signed)
    ]);

    let table = render_three_line_table(
        &[
            "Asset",
            "SpotQty",
            "SpotUSDT",
            "UMNetQty",
            "UMNetUSDT",
            "ExposureQty",
            "ExposureUSDT",
        ],
        &rows,
    );
    info!("现货+UM 敞口汇总\n{}", table);
}

fn log_exposure_summary(
    total_equity: f64,
    total_exposure: f64,
    total_position: f64,
    max_leverage: f64,
) {
    let leverage = if total_equity.abs() <= f64::EPSILON {
        0.0
    } else {
        total_position / total_equity
    };

    let leverage_cell = format!("{} / {}", fmt_decimal(leverage), fmt_decimal(max_leverage));
    let table = render_three_line_table(
        &["TotalEquity", "TotalExposure", "Leverage"],
        &[vec![
            fmt_decimal(total_equity),
            fmt_decimal(total_exposure),
            leverage_cell,
        ]],
    );
    info!("风险指标汇总\n{}", table);
}

fn log_price_table(entries: &BTreeMap<String, PriceEntry>) {
    if entries.is_empty() {
        warn!("未获取到标记价格数据");
        return;
    }

    let rows: Vec<Vec<String>> = entries
        .values()
        .map(|entry| {
            vec![
                entry.symbol.clone(),
                fmt_decimal(entry.mark_price),
                fmt_decimal(entry.index_price),
                entry.update_time.to_string(),
            ]
        })
        .collect();

    let table =
        render_three_line_table(&["Symbol", "MarkPrice", "IndexPrice", "UpdateTime"], &rows);
    info!("标记价格表\n{}", table);
}

fn fmt_decimal(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let mut s = format!("{:.6}", value);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths = compute_widths(headers, rows);
    let mut out = String::new();
    out.push_str(&build_separator(&widths, '-'));
    out.push('\n');
    out.push_str(&build_row(
        headers
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&build_separator(&widths, '='));
    if rows.is_empty() {
        out.push('\n');
        out.push_str(&build_separator(&widths, '-'));
        return out;
    }
    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row.clone(), &widths));
    }
    out.push('\n');
    out.push_str(&build_separator(&widths, '-'));
    out
}

fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                continue;
            }
            widths[idx] = widths[idx].max(cell.len());
        }
    }
    widths
}

fn build_separator(widths: &[usize], fill: char) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&fill.to_string().repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    for (cell, width) in cells.iter().zip(widths.iter()) {
        row.push(' ');
        row.push_str(&format!("{:<width$}", cell, width = *width));
        row.push(' ');
        row.push('|');
    }
    row
}

fn spawn_derivatives_worker(price_table: Rc<RefCell<PriceTable>>) -> Result<()> {
    let service = DERIVATIVES_SERVICE.to_string();
    let node_name = NODE_PRE_TRADE_DERIVATIVES.to_string();
    tokio::task::spawn_local(async move {
        if let Err(err) = derivatives_loop(node_name, service, price_table).await {
            error!("derivatives worker exited: {err:?}");
        }
    });
    Ok(())
}

async fn derivatives_loop(
    node_name: String,
    service: String,
    price_table: Rc<RefCell<PriceTable>>,
) -> Result<()> {
    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()?;

    let service = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
        .open_or_create()?;
    let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
        service.subscriber_builder().create()?;
    info!("derivatives metrics subscribed: service={}", service.name());

    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                let payload = trim_payload(sample.payload());
                if payload.is_empty() {
                    continue;
                }
                let Some(msg_type) = get_msg_type(&payload) else {
                    continue;
                };
                match msg_type {
                    MktMsgType::MarkPrice => match parse_mark_price(&payload) {
                        Ok(msg) => {
                            let mut table = price_table.borrow_mut();
                            table.update_mark_price(&msg.symbol, msg.mark_price, msg.timestamp);
                        }
                        Err(err) => warn!("parse mark price failed: {err:?}"),
                    },
                    MktMsgType::IndexPrice => match parse_index_price(&payload) {
                        Ok(msg) => {
                            let mut table = price_table.borrow_mut();
                            table.update_index_price(&msg.symbol, msg.index_price, msg.timestamp);
                        }
                        Err(err) => warn!("parse index price failed: {err:?}"),
                    },
                    _ => {}
                }
            }
            Ok(None) => {
                tokio::task::yield_now().await;
            }
            Err(err) => {
                warn!("derivatives stream receive error: {err}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}
