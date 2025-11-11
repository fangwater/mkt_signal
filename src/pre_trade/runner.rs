use crate::common::iceoryx_publisher::{
    ResamplePublisher, SignalPublisher, RESAMPLE_PAYLOAD, SIGNAL_PAYLOAD,
};
use crate::common::min_qty_table::MinQtyTable;
use crate::common::time_util::get_timestamp_us;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::SignalBytes;
use crate::strategy::hedge_arb_strategy::HedgeArbStrategy;
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::signal::resample::{
    PreTradeExposureResampleEntry, PreTradeExposureRow, PreTradePositionResampleEntry,
    PreTradeRiskResampleEntry, PreTradeSpotBalanceRow, PreTradeUmPositionRow,

};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::{Strategy, StrategyManager};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iceoryx2::port::{publisher::Publisher, subscriber::Subscriber};
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
        // 初始化策略参数从 Redis
        let strategy_params = StrategyParamsCfg::default();
        let mut runtime = RuntimeContext::new(
            bootstrap,
            order_record_tx.clone(),
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

        let mut trade_resp_rx = spawn_trade_response_listener(&self.cfg.trade_engine)?;
        let mut external_signal_rx = spawn_signal_listeners(&self.cfg.signals)?;

        // 提升周期检查频率到 100ms，使策略状态响应更及时
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    break;
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

struct RuntimeContext {
    order_manager: Rc<RefCell<crate::pre_trade::order_manager::OrderManager>>,
    strategy_mgr: Rc<RefCell<StrategyManager>>,
    strategy_params: StrategyParamsCfg,
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
        order_publisher: OrderPublisher,
        strategy_params: StrategyParamsCfg,
        signal_record_pub: Option<SignalPublisher>,
    ) -> Self {

        Self {
            spot_manager,
            um_manager,
            exposure_manager: exposure_manager_rc,
            price_table,
            order_manager: Rc::new(RefCell::new(
                crate::pre_trade::order_manager::OrderManager::new(order_record_tx.clone()),
            )),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            signal_tx,
            strategy_params,
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
        self.strategy_mgr.borrow_mut().insert(strategy);
    }


    fn remove_strategy(&mut self, strategy_id: i32) {
        self.strategy_mgr.borrow_mut().remove(strategy_id);
    }

    fn with_strategy_mut<F>(&mut self, strategy_id: i32, mut f: F)
    where
        F: FnMut(&mut dyn Strategy),
    {
        if let Some(mut strategy) = self.strategy_mgr.borrow_mut().take(strategy_id) {
            f(strategy.as_mut());
            if strategy.is_active() {
                self.strategy_mgr.borrow_mut().insert(strategy);
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
            self.strategy_params.max_symbol_exposure_ratio,
            self.strategy_params.max_total_exposure_ratio,
            self.strategy_params.max_pos_u,
            self.strategy_params.max_leverage,
            self.max_pending_limit_orders.clone(),
        );

        Rc::new(crate::strategy::risk_checker::PreTradeEnv::new(
            self.min_qty_table.clone(),
            Some(self.signal_tx.clone()),
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
                    self.strategy_params.max_leverage,
                );
            }
        }
    }

    async fn tick(&mut self) {
        let now = get_timestamp_us();
        self.strategy_mgr.borrow_mut().handle_period_clock(now);
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

fn resolve_order_req_service(cfg: &TradeEngineRespCfg) -> String {
    if let Some(req_service) = cfg.req_service.clone() {
        return req_service;
    }
    if cfg.service.contains("order_resps/") {
        return cfg.service.replace("order_resps/", "order_reqs/");
    }
    cfg.service.replace("resps", "reqs")
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
                for strategy_id in candidate_ids {
                    if !ctx.strategy_mgr.borrow().contains(strategy_id) {
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
                if !ctx.strategy_mgr.borrow().contains(strategy_id) {
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

