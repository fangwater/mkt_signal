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
    PreTradeCfg, TradeEngineRespCfg,
};
use crate::pre_trade::event::AccountEvent;
use crate::pre_trade::exposure_manager::{ExposureEntry, ExposureManager};
use crate::pre_trade::price_table::{PriceEntry, PriceTable};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::channels::SIGNAL_CHANNEL_MM_ARBITRAGE_BACKWARD;
use crate::signal::common::{ExecutionType, SignalBytes};
use crate::strategy::hedge_arb_strategy::HedgeArbStrategy;
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::signal::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradePositionResampleEntry,
    PreTradeRiskResampleEntry, PreTradeSpotBalanceRow, PreTradeUmPositionRow
};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::{Strategy, StrategyManager};
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


const NODE_PRE_TRADE_SIGNAL_PREFIX: &str = "signals";

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
            signal_tx.clone(),
            order_publisher,
            signal_record_pub,
        );

        // 首次从 Redis 拉取 pre-trade 参数
        if let Err(err) = runtime.reload_params().await {
            warn!("pre_trade initial params load failed: {err:#}");
        }

        let mut order_rx = order_rx;
        let mut internal_signal_rx = signal_rx;

        // 创建 MonitorChannel 实例并启动 account_pubs/binance_pm 监听
        let mut monitor_channel = crate::pre_trade::monitor_channel::MonitorChannel::new_binance_pm_monitor();
        let mut account_rx = monitor_channel.take_receiver()
            .ok_or_else(|| anyhow::anyhow!("failed to get account receiver"))?;

        let mut trade_resp_rx = spawn_trade_response_listener(&self.cfg.trade_engine)?;
        let mut external_signal_rx = spawn_signal_listeners(&self.cfg.signals)?;

        spawn_derivatives_worker(runtime.price_table.clone())?;

        // 提升周期检查频率到 100ms，使策略状态响应更及时
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
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
    // Risk parameters (loaded from Redis)
    max_pos_u: f64,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_leverage: f64,
    max_pending_limit_orders: Rc<Cell<i32>>,
    min_qty_table: Rc<MinQtyTable>,
    signal_tx: UnboundedSender<Bytes>,
    resample_interval: std::time::Duration,
    next_resample: std::time::Instant,
    next_params_refresh: std::time::Instant,
    params_refresh_secs: u64,
    last_params_snapshot: Option<PreTradeParamsSnap>,
}

impl RuntimeContext {
    fn new(
        bootstrap: BootstrapResources,
        order_record_tx: UnboundedSender<Bytes>,
        signal_tx: UnboundedSender<Bytes>,
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
                crate::pre_trade::order_manager::OrderManager::new(order_record_tx.clone()),
            )),
            strategy_mgr: StrategyManager::new(),
            signal_tx,
            // Default risk parameters
            max_pos_u: 0.0,
            max_symbol_exposure_ratio: 0.25,  // 25%
            max_total_exposure_ratio: 0.25,   // 25%
            max_leverage: 2.0,
            max_pending_limit_orders: Rc::new(Cell::new(3)),
            min_qty_table,
            resample_positions_pub,
            resample_exposure_pub,
            resample_risk_pub,
            signal_record_pub,
            backward_pub,
            resample_interval: std::time::Duration::from_secs(3),
            next_resample: std::time::Instant::now() + std::time::Duration::from_secs(3),
            next_params_refresh: std::time::Instant::now(),
            params_refresh_secs: 30,
            last_params_snapshot: None,
        }
    }

    fn cleanup_inactive(&mut self) {}

    /// 检查交易对的限价挂单数是否达到上限
    /// 返回 true 表示可以继续下单，false 表示已达上限
    fn check_pending_limit_order_quota(&self, symbol: &str) -> bool {
        let max_limit = self.max_pending_limit_orders.get();
        if max_limit <= 0 {
            return true; // 未设置限制
        }

        let current_limit = {
            let manager = self.order_manager.borrow();
            manager.get_symbol_pending_limit_order_count(&symbol.to_string())
        };

        if current_limit >= max_limit {
            warn!(
                "symbol={} 当前限价挂单数={} 已达到上限 {}，忽略开仓信号",
                symbol, current_limit, max_limit
            );
            false
        } else {
            true
        }
    }

    /// 查询指定 symbol 在指定 venue 的头寸数量（带符号）
    /// 正数表示多头，负数表示空头
    fn get_position_qty(&self, symbol: &str, venue: u8) -> f64 {
        use crate::signal::common::TradingVenue;

        let Some(trading_venue) = TradingVenue::from_u8(venue) else {
            return 0.0;
        };

        match trading_venue {
            TradingVenue::BinanceUm => {
                // 查询合约头寸（带符号）
                if let Some(snapshot) = self.um_manager.snapshot() {
                    snapshot
                        .positions
                        .iter()
                        .find(|p| p.symbol.eq_ignore_ascii_case(symbol))
                        .map(|p| p.position_amt)
                        .unwrap_or(0.0)
                } else {
                    0.0
                }
            }
            TradingVenue::BinanceMargin => {
                // 查询现货全仓杠杆头寸（带符号）
                if let Some(snapshot) = self.spot_manager.snapshot() {
                    snapshot
                        .balances
                        .iter()
                        .find(|b| b.asset.eq_ignore_ascii_case(symbol))
                        .map(|b| b.net_asset())
                        .unwrap_or(0.0)
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }

    fn insert_strategy(&mut self, strategy: Box<dyn Strategy>) {
        self.strategy_mgr.insert(strategy);
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
        self.order_record_tx.clone()
    }

    fn signal_sender(&self) -> UnboundedSender<Bytes> {
        self.signal_tx.clone()
    }

    fn get_pre_trade_env(&self) -> Rc<crate::strategy::risk_checker::PreTradeEnv> {
        let risk_checker = crate::strategy::risk_checker::RiskChecker::new(
            self.exposure_manager.clone(),
            self.order_manager.clone(),
            self.price_table.clone(),
            self.max_symbol_exposure_ratio,
            self.max_total_exposure_ratio,
            self.max_pos_u,
            self.max_leverage,
            self.max_pending_limit_orders.clone(),
        );

        Rc::new(crate::strategy::risk_checker::PreTradeEnv::new(
            self.min_qty_table.clone(),
            Some(self.signal_tx.clone()),
            None, // signal_query_tx 暂时不需要
            self.order_record_tx.clone(),
            risk_checker,
        ))
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
                    self.max_leverage,
                );
            }
        }
    }

    async fn tick(&mut self) {
        let now = get_timestamp_us();
        self.strategy_mgr.handle_period_clock(now);
        self.cleanup_inactive();
        let instant_now = std::time::Instant::now();
        if instant_now >= self.next_params_refresh {
            match self.reload_params().await {
                Ok(()) => {
                }
                Err(err) => {
                    warn!("pre_trade params refresh failed: {err:#}");
                }
            }
            self.next_params_refresh =
                instant_now + std::time::Duration::from_secs(self.params_refresh_secs.max(5));
        }

        if self.resample_positions_pub.is_some()
            || self.resample_exposure_pub.is_some()
            || self.resample_risk_pub.is_some()
        {
            while instant_now >= self.next_resample {
                if let Err(err) = self.publish_resample_entries() {
                    warn!("pre_trade resample publish failed: {err:#}");
                    self.next_resample = std::time::Instant::now() + self.resample_interval;
                    break;
                }
                self.next_resample += self.resample_interval;
            }
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

        // Update risk parameters from Redis (if keys exist)
        if let Some(v) = parse_f64("pre_trade_max_pos_u") {
            self.max_pos_u = v;
        }
        if let Some(v) = parse_f64("pre_trade_max_symbol_exposure_ratio") {
            self.max_symbol_exposure_ratio = v;
        }
        if let Some(v) = parse_f64("pre_trade_max_total_exposure_ratio") {
            self.max_total_exposure_ratio = v;
        }
        if let Some(v) = parse_f64("pre_trade_max_leverage") {
            if v > 0.0 {
                self.max_leverage = v;
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
            max_pos_u: self.max_pos_u,
            max_symbol_exposure_ratio: self.max_symbol_exposure_ratio,
            max_total_exposure_ratio: self.max_total_exposure_ratio,
            max_leverage: self.max_leverage,
            refresh_secs: new_refresh,
        };
        let changed = self
            .last_params_snapshot
            .as_ref()
            .map(|old| old != &snapshot)
            .unwrap_or(true);

        self.params_refresh_secs = new_refresh;
        self.last_params_snapshot = Some(snapshot);
        if changed {
            debug!(
                "pre_trade params updated: max_pos_u={:.2} sym_ratio={:.4} total_ratio={:.4} max_leverage={:.2} refresh={}s",
                self.max_pos_u,
                self.max_symbol_exposure_ratio,
                self.max_total_exposure_ratio,
                self.max_leverage,
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
        let max_leverage = self.max_leverage;
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


fn handle_trade_signal(ctx: &mut RuntimeContext, signal: TradeSignal) {
    match signal.signal_type {
        SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
            Ok(open_ctx) => {
                let symbol = open_ctx.get_opening_symbol().to_uppercase();
                if !ctx.check_pending_limit_order_quota(&symbol) {
                    return;
                }
                let strategy_id = StrategyManager::generate_strategy_id();
                let env = ctx.get_pre_trade_env();
                let mut strategy = HedgeArbStrategy::new(strategy_id, symbol.clone(), env);
                strategy.handle_signal(&signal);
                if strategy.is_active() {
                    ctx.insert_strategy(Box::new(strategy));
                }
            }
            Err(err) => warn!("failed to decode ArbOpen context: {err}"),
        },
        SignalType::ArbClose => {
            match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(close_ctx) => {
                    use crate::pre_trade::order_manager::Side;

                    let opening_symbol = close_ctx.get_opening_symbol();
                    let hedging_symbol = close_ctx.get_hedging_symbol();

                    // 获取平仓方向
                    let Some(close_side) = Side::from_u8(close_ctx.side) else {
                        warn!("ArbClose: invalid side {}", close_ctx.side);
                        return;
                    };

                    // 查询两条腿的持仓（带符号）
                    let opening_pos =
                        ctx.get_position_qty(&opening_symbol, close_ctx.opening_leg.venue);
                    let hedging_pos =
                        ctx.get_position_qty(&hedging_symbol, close_ctx.hedging_leg.venue);

                    // 检查opening leg方向是否匹配
                    // 如果close是Sell，持仓应该>0（多头）；如果close是Buy，持仓应该<0（空头）
                    let opening_direction_match = match close_side {
                        Side::Sell => opening_pos > 0.0,
                        Side::Buy => opening_pos < 0.0,
                    };

                    // 检查hedging leg方向是否匹配（方向相反）
                    // 如果opening close是Sell，hedging close应该是Buy，持仓应该<0（空头）
                    let hedging_direction_match = match close_side {
                        Side::Sell => hedging_pos < 0.0,
                        Side::Buy => hedging_pos > 0.0,
                    };

                    if !opening_direction_match || !hedging_direction_match {
                        warn!(
                            "ArbClose: position direction mismatch, close_side={:?} opening_symbol={} opening_pos={:.6} hedging_symbol={} hedging_pos={:.6}",
                            close_side, opening_symbol, opening_pos, hedging_symbol, hedging_pos
                        );
                        return;
                    }

                    // 两条腿方向都匹配，取绝对值的最小值
                    let closeable_qty = opening_pos.abs().min(hedging_pos.abs());

                    // 和信号中的amount对比，取较小值
                    let final_qty = closeable_qty.min(close_ctx.amount as f64);

                    if final_qty <= 0.0 {
                        warn!(
                            "ArbClose: final_qty <= 0, closeable_qty={:.6} signal_amount={:.6}",
                            closeable_qty, close_ctx.amount
                        );
                        return;
                    }

                    debug!(
                        "ArbClose: final_qty={:.6} (closeable={:.6} signal_amount={:.6}) opening_symbol={} opening_pos={:.6} hedging_symbol={} hedging_pos={:.6}",
                        final_qty, closeable_qty, close_ctx.amount, opening_symbol, opening_pos, hedging_symbol, hedging_pos
                    );

                    // 平仓本质就是反向开仓，复用 HedgeArbStrategy
                    let strategy_id = StrategyManager::generate_strategy_id();
                    let env = ctx.get_pre_trade_env();

                    let mut strategy =
                        HedgeArbStrategy::new(strategy_id, opening_symbol.clone(), env);

                    strategy.handle_signal(&signal);

                    if strategy.is_active() {
                        ctx.insert_strategy(Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode ArbClose context: {err}"),
            }
        }
        SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
            Ok(cancel_ctx) => {
                let symbol = cancel_ctx.get_opening_symbol().to_uppercase();
                let candidate_ids: Vec<i32> = ctx
                    .strategy_mgr
                    .ids_for_symbol(&symbol)
                    .map(|set| set.iter().copied().collect())
                    .unwrap_or_default();

                if candidate_ids.is_empty() {
                    return;
                }
                let payload = cancel_ctx.to_bytes();
                for strategy_id in candidate_ids {
                    let record = SignalRecordMessage::new(
                        strategy_id,
                        signal.signal_type.clone(),
                        payload.clone().to_vec(),
                        signal.generation_time,
                    );
                    ctx.publish_signal_record(&record);
                    if !ctx.strategy_mgr.contains(strategy_id) {
                        return;
                    }
                    ctx.with_strategy_mut(strategy_id, |strategy| {
                        strategy.handle_signal(&signal);
                    });
                }
            }
            Err(err) => warn!("failed to decode ArbCancel context: {err}"),
        },
        SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
            Ok(hedge_ctx) => {
                let strategy_id = hedge_ctx.strategy_id;
                let record = SignalRecordMessage::new(
                    strategy_id,
                    SignalType::ArbHedge,
                    signal.context.clone().to_vec(),
                    signal.generation_time,
                );
                ctx.publish_signal_record(&record);
                if !ctx.strategy_mgr.contains(strategy_id) {
                    return;
                }
                ctx.with_strategy_mut(strategy_id, |strategy| {
                    strategy.handle_signal(&signal);
                });
            }
            Err(err) => warn!("failed to decode hedge context: {err}"),
        },
    }

    ctx.cleanup_inactive();
}


fn dispatch_execution_report(ctx: &mut RuntimeContext, report: &ExecutionReportMsg) {
    let order_id = report.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match report.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update(report);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update(report);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            report.execution_type(),
                            report.symbol,
                            report.client_order_id,
                            report.order_id
                        );
                        strategy.apply_order_update(report);
                    }
                    _ => {
                        error!(
                            "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                            report.execution_type(),
                            report.symbol,
                            report.client_order_id,
                            report.order_id
                        );
                    }
                }
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
    let order_id: i64 = update.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match update.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update(update);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update(update);
                    }
                    ExecutionType::Expired | ExecutionType::Rejected => {
                        warn!(
                            "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            update.symbol,
                            update.client_order_id,
                            update.order_id
                        );
                        strategy.apply_order_update(update);
                    }
                    _ => {
                        error!(
                            "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                            update.execution_type(),
                            update.symbol,
                            update.client_order_id,
                            update.order_id
                        );
                    }
                }
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
                let payload = Bytes::copy_from_slice(sample.payload());
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
