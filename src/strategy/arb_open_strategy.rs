use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::{PersistChannel, QueryEngHub, TradeEngHub};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::{
    ArbOpenPriceMapEntry, ForceCloseControl, HedgeOrphanHandoff, HedgeOrphanSourceKind, Strategy,
};
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_reconcile::{qv_decimal_or_fallback, ORDER_QUERY_WATCHDOG_DELAY_US};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_order_updates::OrderQueryTradeUpdate;
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_arb_publish::{
    publish_arb_uniform_new_order, publish_arb_uniform_terminal_order,
    publish_arb_uniform_trade_order, publish_arb_uniform_trade_order_from_order_update,
    ArbUniformPublishCtx,
};
use crate::strategy::ws_order_update::WsOrderUpdate;
use log::{debug, error, info, warn};
use std::any::Any;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingOrderQueryReason {
    OrderWatchdog,
    CancelWatchdog,
    CancelRejected,
}

impl PendingOrderQueryReason {
    fn is_cancel_rejected(self) -> bool {
        matches!(self, Self::CancelRejected)
    }

    fn watchdog_hint(self) -> &'static str {
        if self.is_cancel_rejected() {
            "CancelRejectedWatchdog触发"
        } else {
            "CancelWatchdog触发"
        }
    }

    fn query_send_failed_trigger(self) -> &'static str {
        if self.is_cancel_rejected() {
            "cancel_rejected_query_send_failed"
        } else {
            "cancel_query_send_failed"
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryWatchdog {
    client_order_id: i64,
    due_ts_us: i64,
    reason: PendingOrderQueryReason,
}

/// 单腿套利开仓策略：只负责 open leg 生命周期，不保存 hedge leg 或双腿盘口。
pub struct ArbOpenStrategy {
    strategy_id: i32,
    open_symbol: String,
    open_venue: TradingVenue,
    open_order_id: i64,
    open_expire_ts: Option<i64>,
    open_side: Option<Side>,
    open_signal_ts: i64,
    open_from_key: String,
    open_price_qv: QuantizedValue,
    open_price_offset: f64,
    pub close_ts: Option<i64>,    //仓位希望持有的时间
    pub cumulative_open_qty: f64, //累计开仓数量
    pub open_qty_multiplier: f64, //开仓侧数量乘数（venue qty -> base qty）
    alive_flag: bool,
    pending_order_query: Option<PendingOrderQueryReason>,
    order_query_watchdog: Option<QueryWatchdog>,
    cancel_query_watchdog: Option<QueryWatchdog>,
    last_cancel_trigger_ts: Option<i64>,
    last_open_cancel_reason: Option<&'static str>,
}

impl ArbOpenStrategy {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            strategy_id,
            open_symbol: String::new(),
            open_venue: TradingVenue::BinanceMargin,
            open_order_id: 0,
            open_expire_ts: None,
            open_side: None,
            open_signal_ts: 0,
            open_from_key: String::new(),
            open_price_qv: QuantizedValue::zero(),
            open_price_offset: 0.0,
            close_ts: None,
            cumulative_open_qty: 0.0,
            open_qty_multiplier: 1.0,
            alive_flag: true,
            pending_order_query: None,
            order_query_watchdog: None,
            cancel_query_watchdog: None,
            last_cancel_trigger_ts: None,
            last_open_cancel_reason: None,
        }
    }

    pub fn open_side(&self) -> Option<Side> {
        self.open_side
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号。
    fn compose_order_id(strategy_id: i32) -> i64 {
        ((strategy_id as i64) << 32) | 1
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn preview_text(raw: &str, max_chars: usize) -> String {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return "-".to_string();
        }
        let mut out = String::new();
        for (idx, ch) in trimmed.chars().enumerate() {
            if idx >= max_chars {
                out.push_str("...");
                break;
            }
            out.push(ch);
        }
        out
    }

    fn release_open_order_keep_local(&mut self, client_order_id: i64, reason: &str) {
        self.pending_order_query = None;
        self.clear_query_watchdogs(client_order_id);
        self.open_order_id = 0;
        warn!(
            "ArbOpenStrategy: strategy_id={} release open order keep local client_order_id={} reason={}",
            self.strategy_id, client_order_id, reason
        );
    }

    fn handoff_open_order_to_hedge_orphan(&mut self, client_order_id: i64, reason: &str) -> bool {
        if client_order_id <= 0 {
            self.alive_flag = false;
            return false;
        }
        warn!(
            "ArbOpenStrategy: strategy_id={} hedge_orphan_handoff_start client_order_id={} reason={}",
            self.strategy_id, client_order_id, reason
        );
        let handoff = HedgeOrphanHandoff {
            client_order_id,
            source_strategy_id: self.strategy_id,
            source_kind: HedgeOrphanSourceKind::Open,
            uniform_ctx: self.uniform_open_publish_ctx(),
            recorded_base_qty: self.cumulative_open_qty,
            reason: reason.to_string(),
        };
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "ArbOpenStrategy: strategy_id={} orphan manager unavailable client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };
        let adopted = orphan_mgr
            .borrow_mut()
            .adopt_hedge_orphan_order_id(&handoff);
        if !adopted {
            warn!(
                "ArbOpenStrategy: strategy_id={} hedge orphan handoff rejected client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        }
        self.release_open_order_keep_local(client_order_id, reason);
        self.alive_flag = false;
        true
    }

    fn cleanup_strategy_orders(&mut self) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let mut mgr = order_mgr.borrow_mut();
        if self.open_order_id != 0 {
            if let Some(order) = mgr.get(self.open_order_id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(
                        &order,
                        "ArbOpen开仓订单未达到终结状态被清理",
                        self.strategy_id,
                    );
                }
            }
            let _ = mgr.remove(self.open_order_id);
        }
    }

    fn terminalize_open_order_before_cleanup(&mut self, client_order_id: i64) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let event_time = get_timestamp_us();
        let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
            if order.status.is_terminal() {
                return;
            }
            order.status = OrderExecutionStatus::Rejected;
            order.set_end_time(event_time);
        });
    }

    fn handle_open_failed_cleanup(&mut self, client_order_id: i64) {
        self.pending_order_query = None;
        self.clear_query_watchdogs(client_order_id);
        self.terminalize_open_order_before_cleanup(client_order_id);
        self.cleanup_strategy_orders();
        self.alive_flag = false;
    }

    fn try_apply_ws_order_update(&mut self, response: &dyn TradeEngineResponse) -> bool {
        if !WsOrderUpdate::supports_trade_response_req_type(response.req_type()) {
            return false;
        }

        let client_order_id = response.client_order_id();
        if client_order_id != self.open_order_id {
            return false;
        }
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order_snapshot) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} ws order update missing local order: client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };

        let Some(update) = WsOrderUpdate::from_trade_response(response, &order_snapshot) else {
            return false;
        };

        if matches!(
            order_snapshot.venue,
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
        ) {
            if matches!(update.status(), OrderStatus::New | OrderStatus::Canceled) {
                <Self as Strategy>::apply_order_update(self, &update);
            } else {
                debug!(
                    "ArbOpenStrategy: strategy_id={} skip non-NEW/CANCELED binance ws response: venue={:?} client_order_id={} status={:?}",
                    self.strategy_id,
                    order_snapshot.venue,
                    client_order_id,
                    update.status()
                );
            }
            return true;
        }

        if matches!(
            update.status(),
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
            let trade = OrderQueryTradeUpdate::new(
                &order_snapshot,
                update.order_id(),
                update.event_time(),
                update.cumulative_filled_quantity(),
                response.response_price(),
                Some(update.status()),
                update.time_in_force(),
            );
            <Self as Strategy>::apply_trade_update(self, &trade);
        } else {
            <Self as Strategy>::apply_order_update(self, &update);
        }
        true
    }

    fn handle_arb_open_signal(&mut self, ctx: ArbOpenCtx) {
        let symbol = normalize_symbol_for_internal(&ctx.get_opening_symbol());
        if symbol.is_empty() {
            warn!(
                "ArbOpenStrategy: strategy_id={} empty symbol",
                self.strategy_id
            );
            self.alive_flag = false;
            return;
        }

        let Some(venue) = TradingVenue::from_u8(ctx.opening_leg.venue) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} invalid venue={}",
                self.strategy_id, ctx.opening_leg.venue
            );
            self.alive_flag = false;
            return;
        };
        let Some(side) = Side::from_u8(ctx.side) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} invalid side={}",
                self.strategy_id, ctx.side
            );
            self.alive_flag = false;
            return;
        };
        let Some(order_type) = OrderType::from_u8(ctx.order_type) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} invalid order_type={}",
                self.strategy_id, ctx.order_type
            );
            self.alive_flag = false;
            return;
        };

        let qty = ctx.amount_value();
        if qty <= 0.0 {
            warn!(
                "ArbOpenStrategy: strategy_id={} invalid qty={}",
                self.strategy_id, qty
            );
            self.alive_flag = false;
            return;
        }
        let signal_price = ctx.price_value();
        if signal_price <= 0.0 {
            warn!(
                "ArbOpenStrategy: strategy_id={} invalid price={} order_type={:?}",
                self.strategy_id, signal_price, order_type
            );
            self.alive_flag = false;
            return;
        }
        if ctx.price_count() <= 0 || ctx.amount_count() <= 0 {
            warn!(
                "ArbOpenStrategy: strategy_id={} invalid ArbOpen qv count price_count={} amount_count={}",
                self.strategy_id,
                ctx.price_count(),
                ctx.amount_count()
            );
            self.alive_flag = false;
            return;
        }
        if !venue.supports_pre_trade_stack() {
            panic!(
                "ArbOpenStrategy: strategy_id={} 不支持的交易场所 {:?}，仅支持 Binance/OKX/Bybit/Bitget/Gate 的 futures 或 margin",
                self.strategy_id, venue
            );
        }

        if let Err(e) = MonitorChannel::instance().check_symbol_exposure(&symbol) {
            error!(
                "ArbOpenStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id, symbol, e
            );
            self.alive_flag = false;
            return;
        }
        if let Err(e) = MonitorChannel::instance().check_total_exposure() {
            error!(
                "ArbOpenStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }
        if order_type == OrderType::Limit {
            if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol, side) {
                error!(
                    "ArbOpenStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃",
                    self.strategy_id, symbol, e
                );
                self.alive_flag = false;
                return;
            }
        }

        let rate_params = PreTradeParamsLoader::instance();
        if let Err(e) = OrderRateLimiter::check_limit(
            OrderRateBucket::MmOpen,
            rate_params.open_order_rate_limit_per_min(),
            rate_params.open_order_rate_limit_10s(),
            get_timestamp_us(),
        ) {
            info!(
                "ArbOpenStrategy: strategy_id={} symbol={} 开仓下单频率风控触发: {}，标记策略为不活跃",
                self.strategy_id, symbol, e
            );
            self.alive_flag = false;
            return;
        }

        let qty_multiplier = match MonitorChannel::instance()
            .qty_multiplier_for_venue(venue, &symbol)
        {
            Ok(multiplier) => multiplier,
            Err(err) => {
                error!(
                    "ArbOpenStrategy: strategy_id={} 初始化开仓数量乘数失败 symbol={} venue={:?}: {}",
                    self.strategy_id, symbol, venue, err
                );
                self.alive_flag = false;
                return;
            }
        };

        let order_qty = qty;
        let order_price = signal_price;
        let signed_qty = match side {
            Side::Buy => order_qty.abs(),
            Side::Sell => -order_qty.abs(),
        };
        let add_base_qty = signed_qty * qty_multiplier;
        let current_base_qty = MonitorChannel::instance().get_position_qty(&symbol, venue);
        let projected_base_qty = current_base_qty + add_base_qty;
        if projected_base_qty.abs() > current_base_qty.abs() + 1e-12_f64 {
            if let Err(e) = MonitorChannel::instance().check_leverage() {
                error!(
                    "ArbOpenStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                    self.strategy_id, e
                );
                self.alive_flag = false;
                return;
            }
        }
        if let Err(e) =
            MonitorChannel::instance().ensure_max_pos_u(&symbol, signed_qty, order_price)
        {
            error!(
                "ArbOpenStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        self.open_symbol = symbol.clone();
        self.open_venue = venue;
        self.open_expire_ts = (ctx.exp_time > 0).then_some(ctx.exp_time);
        self.open_side = Some(side);
        self.open_signal_ts = ctx.create_ts;
        self.open_from_key = String::from_utf8_lossy(&ctx.from_key).to_string();
        self.open_price_qv = ctx.price_qv;
        self.open_price_offset = ctx.price_offset;
        self.close_ts = if ctx.hedge_timeout_us > 0 {
            let base_ts = if ctx.create_ts > 0 {
                ctx.create_ts
            } else {
                get_timestamp_us()
            };
            Some(base_ts.saturating_add(ctx.hedge_timeout_us))
        } else {
            None
        };
        self.cumulative_open_qty = 0.0;
        self.open_qty_multiplier = qty_multiplier;

        let client_order_id = Self::compose_order_id(self.strategy_id);
        self.open_order_id = client_order_id;

        let submit_ts = get_timestamp_us();
        MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order(
                venue,
                client_order_id,
                order_type,
                symbol.clone(),
                side,
                order_qty,
                order_price,
                false,
                qty_multiplier,
                submit_ts,
            );

        info!(
            "📤 ArbOpen订单已创建: strategy_id={} client_order_id={} symbol={} {:?} side={:?} qty={} price={} qty_multiplier={:.8} close_ts={:?} from_key_len={}",
            self.strategy_id,
            client_order_id,
            symbol,
            venue,
            side,
            qv_decimal_or_fallback(order_qty),
            qv_decimal_or_fallback(order_price),
            qty_multiplier,
            self.close_ts,
            ctx.from_key_len
        );

        if let Err(err) = self.create_and_send_order(client_order_id, &symbol) {
            error!(
                "ArbOpenStrategy: strategy_id={} open order send failed: {}",
                self.strategy_id, err
            );
        } else {
            info!(
                "✅ ArbOpen订单已发送: strategy_id={} client_order_id={}",
                self.strategy_id, client_order_id
            );
        }
    }

    fn handle_open_leg_timeout(&mut self) {
        let Some(expire_ts) = self.open_expire_ts else {
            return;
        };
        let now = get_timestamp_us();
        if now < expire_ts || !self.alive_flag || self.open_order_id == 0 {
            return;
        }

        info!(
            "ArbOpenStrategy: strategy_id={} 开仓订单超时，直接撤单 order_id={}",
            self.strategy_id, self.open_order_id
        );
        self.last_open_cancel_reason = Some("timeout");

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);
        if let Some(order) = order {
            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        info!(
                            "ArbOpenStrategy: strategy_id={} exchange={} 发送开仓撤单请求失败: {}",
                            self.strategy_id, exchange, e
                        );
                    } else {
                        info!(
                            "ArbOpenStrategy: strategy_id={} exchange={} reason=timeout 已发送开仓撤单请求 order_id={}",
                            self.strategy_id, exchange, self.open_order_id
                        );
                        self.open_expire_ts = None;
                        self.schedule_cancel_query_watchdog(self.open_order_id);
                    }
                }
                Err(e) => error!(
                    "ArbOpenStrategy: strategy_id={} 获取开仓撤单请求字节失败: {}",
                    self.strategy_id, e
                ),
            }
        }
    }

    fn handle_arb_cancel_signal(&mut self, ctx: ArbCancelCtx) {
        let precise_target = ctx.strategy_id > 0;
        let from_key_preview = Self::preview_text(&String::from_utf8_lossy(&ctx.from_key), 160);
        if self.last_cancel_trigger_ts == Some(ctx.trigger_ts) {
            debug!(
                "ArbOpenStrategy: strategy_id={} skip duplicate ArbCancel trigger_ts={} open_order_id={} from_key='{}'",
                self.strategy_id, ctx.trigger_ts, self.open_order_id, from_key_preview
            );
            return;
        }
        if precise_target && ctx.strategy_id != self.strategy_id {
            info!(
                "ArbOpenStrategy: strategy_id={} ignore targeted ArbCancel target_strategy_id={} trigger_ts={} from_key='{}'",
                self.strategy_id, ctx.strategy_id, ctx.trigger_ts, from_key_preview
            );
            return;
        }
        if !precise_target {
            let cancel_side = ctx.get_side();
            if let Some(open_side) = self.open_side {
                if open_side != cancel_side {
                    info!(
                        "ArbOpenStrategy: strategy_id={} skip ArbCancel due to side mismatch open_side={:?} cancel_side={:?} open_order_id={} trigger_ts={} from_key='{}'",
                        self.strategy_id,
                        open_side,
                        cancel_side,
                        self.open_order_id,
                        ctx.trigger_ts,
                        from_key_preview
                    );
                    return;
                }
            }
        }

        if self.pending_order_query.is_some()
            || self
                .cancel_query_watchdog
                .is_some_and(|w| w.client_order_id == self.open_order_id)
        {
            debug!(
                "ArbOpenStrategy: strategy_id={} skip ArbCancel because cancel reconcile already in flight open_order_id={} trigger_ts={} from_key='{}'",
                self.strategy_id, self.open_order_id, ctx.trigger_ts, from_key_preview
            );
            self.last_cancel_trigger_ts = Some(ctx.trigger_ts);
            return;
        }
        if self.open_order_id == 0 {
            info!(
                "ArbOpenStrategy: strategy_id={} skip ArbCancel because open_order_id=0 trigger_ts={} from_key='{}'",
                self.strategy_id, ctx.trigger_ts, from_key_preview
            );
            return;
        }

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_order_id);
        if let Some(order) = order {
            if order.status.is_terminal() {
                info!(
                    "ArbOpenStrategy: strategy_id={} open order already terminal {:?}, skip cancel order_id={} trigger_ts={} from_key='{}'",
                    self.strategy_id, order.status, self.open_order_id, ctx.trigger_ts, from_key_preview
                );
                return;
            }

            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    let cancel_reason = ctx.get_reason().as_log_reason();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        error!(
                            "ArbOpenStrategy: strategy_id={} exchange={} 发送撤单请求失败 order_id={} trigger_ts={} from_key='{}' err={}",
                            self.strategy_id, exchange, self.open_order_id, ctx.trigger_ts, from_key_preview, e
                        );
                    } else {
                        self.last_open_cancel_reason = Some(cancel_reason);
                        self.last_cancel_trigger_ts = Some(ctx.trigger_ts);
                        self.schedule_cancel_query_watchdog(order.client_order_id);
                        info!(
                            "ArbOpenStrategy: strategy_id={} exchange={} reason={} 已发送开仓撤单请求 order_id={} trigger_ts={} from_key='{}'",
                            self.strategy_id,
                            exchange,
                            cancel_reason,
                            self.open_order_id,
                            ctx.trigger_ts,
                            from_key_preview
                        );
                    }
                }
                Err(e) => error!(
                    "ArbOpenStrategy: strategy_id={} 获取撤单请求字节失败 order_id={} trigger_ts={} from_key='{}' err={}",
                    self.strategy_id, self.open_order_id, ctx.trigger_ts, from_key_preview, e
                ),
            }
        }
    }

    fn create_and_send_order(&mut self, client_order_id: i64, symbol: &str) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id);
        let Some(order) = order else {
            self.alive_flag = false;
            return Err(format!(
                "order not found: client_order_id={}",
                client_order_id
            ));
        };

        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_request_bytes() {
            Ok(req_bin) => {
                let stats = OrderRateLimiter::record(
                    OrderRateBucket::MmOpen,
                    client_order_id,
                    get_timestamp_us(),
                );
                info!(
                    "ArbOpenStrategy: strategy_id={} arb open order action recorded client_order_id={} count_10s={} count_1m={}",
                    self.strategy_id, client_order_id, stats.count_10s, stats.count_1m
                );
                if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                    self.alive_flag = false;
                    return Err(format!(
                        "publish order request failed: symbol={} exchange={} err={}",
                        symbol, exchange, e
                    ));
                }
                self.schedule_order_query_watchdog(client_order_id);
                Ok(())
            }
            Err(e) => {
                self.alive_flag = false;
                Err(format!("get order request bytes failed: {}", e))
            }
        }
    }

    fn record_arb_open_fill_delta(
        &self,
        side: Side,
        base_qty_delta: f64,
        fill_ts: i64,
        price: f64,
        update_detail: &str,
    ) {
        if base_qty_delta <= 1e-12 {
            return;
        }
        let close_ts = self.close_ts.unwrap_or(0);
        let updated = MonitorChannel::instance()
            .strategy_mgr()
            .borrow_mut()
            .record_arb_open_fill(
                &self.open_symbol,
                side,
                base_qty_delta,
                fill_ts,
                price,
                close_ts,
            );
        if !updated {
            warn!(
                "ArbOpenStrategy: strategy_id={} record arb hedge open fill failed symbol={} side={:?} base_qty_delta={:.8} fill_ts={} price={:.8} close_ts={} detail={}",
                self.strategy_id,
                self.open_symbol,
                side,
                base_qty_delta,
                fill_ts,
                price,
                close_ts,
                update_detail
            );
        }
    }

    fn schedule_order_query_watchdog(&mut self, client_order_id: i64) {
        self.order_query_watchdog = Some(QueryWatchdog {
            client_order_id,
            due_ts_us: get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US),
            reason: PendingOrderQueryReason::OrderWatchdog,
        });
    }

    fn schedule_cancel_query_watchdog(&mut self, client_order_id: i64) {
        self.cancel_query_watchdog = Some(QueryWatchdog {
            client_order_id,
            due_ts_us: get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US),
            reason: PendingOrderQueryReason::CancelWatchdog,
        });
        if self
            .order_query_watchdog
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.order_query_watchdog = None;
        }
    }

    fn clear_query_watchdogs(&mut self, client_order_id: i64) {
        if client_order_id == self.open_order_id {
            self.pending_order_query = None;
        }
        if self
            .order_query_watchdog
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.order_query_watchdog = None;
        }
        if self
            .cancel_query_watchdog
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.cancel_query_watchdog = None;
        }
    }

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) -> bool {
        if let Some(existing) = self.pending_order_query {
            if reason.is_cancel_rejected() && !existing.is_cancel_rejected() {
                self.pending_order_query = Some(PendingOrderQueryReason::CancelRejected);
            }
            return true;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };

        match build_order_query_request(&order, client_order_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "ArbOpenStrategy: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.strategy_id, exchange, client_order_id, reason, err
                    );
                    return false;
                }
                self.pending_order_query = Some(reason);
                debug!(
                    "ArbOpenStrategy: strategy_id={} order query sent: exchange={} client_order_id={} reason={:?}",
                    self.strategy_id, exchange, client_order_id, reason
                );
                true
            }
            Err(err) => {
                warn!(
                    "ArbOpenStrategy: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
                    self.strategy_id, client_order_id, reason, err
                );
                false
            }
        }
    }

    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    ) {
        warn!(
            "ArbOpenStrategy: strategy_id={} order query {} failed, handoff to hedge orphan: client_order_id={}",
            self.strategy_id, marker, client_order_id
        );
        self.handoff_open_order_to_hedge_orphan(client_order_id, marker);
    }

    fn handle_query_watchdogs(&mut self) {
        let now = get_timestamp_us();
        if let Some(w) = self.cancel_query_watchdog {
            if now >= w.due_ts_us {
                self.cancel_query_watchdog = None;
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let scheduled_at = w.due_ts_us.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    info!(
                        "{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到撤单/终态回报，发送order query回补 reason={:?}",
                        w.reason.watchdog_hint(),
                        self.strategy_id,
                        w.client_order_id,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        w.reason
                    );
                    if !self.send_order_query(w.client_order_id, w.reason) {
                        self.handoff_open_order_to_hedge_orphan(
                            w.client_order_id,
                            w.reason.query_send_failed_trigger(),
                        );
                    }
                }
            }
        }

        if let Some(w) = self.order_query_watchdog {
            if now >= w.due_ts_us {
                self.order_query_watchdog = None;
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let scheduled_at = w.due_ts_us.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    info!(
                        "OrderWatchdog触发: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报，发送order query回补",
                        self.strategy_id,
                        w.client_order_id,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms
                    );
                    if !self.send_order_query(w.client_order_id, w.reason) {
                        self.handoff_open_order_after_query_failure(
                            w.client_order_id,
                            "order query send failed",
                        );
                    }
                }
            }
        }
    }

    fn uniform_open_publish_ctx(&self) -> ArbUniformPublishCtx {
        ArbUniformPublishCtx {
            signal_ts: self.open_signal_ts,
            from_key: self.open_from_key.clone().into_bytes(),
            price_offset: self.open_price_offset,
        }
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_arb_uniform_new_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ArbOpenStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_terminal_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_arb_uniform_terminal_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ArbOpenStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order(
        &self,
        trade: &dyn TradeUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_arb_uniform_trade_order(
            trade,
            order,
            prev_cumulative_filled_qty,
            status,
            &ctx,
            "ArbOpenStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order_from_order_update(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_arb_uniform_trade_order_from_order_update(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ArbOpenStrategy",
            self.strategy_id,
        );
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        let client_order_id = order_update.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_order_id {
            debug!(
                "ArbOpenStrategy: strategy_id={} ignore order_update client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} order update not found client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };
        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            "ArbOpenStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let qty_multiplier = current_order.qty_multiplier;
        let order_side = current_order.side;
        let order_price = current_order.price;
        let incoming_cumulative_filled_qty = order_update.cumulative_filled_quantity();
        let protected_cumulative_fill =
            current_order.protected_cumulative_fill(incoming_cumulative_filled_qty);
        if protected_cumulative_fill.rollback_detected {
            warn!(
                "ArbOpenStrategy: strategy_id={} protected cumulative fill rollback: client_order_id={} symbol={} status={:?} prev={:.8} incoming={:.8} effective={:.8}",
                self.strategy_id,
                current_order.client_order_id,
                current_order.symbol,
                order_update.status(),
                current_order.cumulative_filled_quantity,
                incoming_cumulative_filled_qty,
                protected_cumulative_fill.effective_cum
            );
        }
        let effective_cumulative_filled_qty = protected_cumulative_fill.effective_cum;

        let updated = order_manager.update(client_order_id, |order| match order_update.status() {
            OrderStatus::New => {
                if !self.alive_flag {
                    warn!(
                        "ArbOpenStrategy: strategy_id={} revive on delayed open NEW: client_order_id={} exchange_order_id={} symbol={}",
                        self.strategy_id,
                        client_order_id,
                        order_update.order_id(),
                        order.symbol
                    );
                    self.alive_flag = true;
                }
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
                let cancel_reason = self.last_open_cancel_reason.unwrap_or("unknown");
                info!(
                    "🚫 ArbOpen订单已撤销: strategy_id={} client_order_id={} symbol={} reason={} filled={}/{}",
                    self.strategy_id,
                    client_order_id,
                    order.symbol,
                    cancel_reason,
                    qv_decimal_or_fallback(order.cumulative_filled_quantity),
                    qv_decimal_or_fallback(order.quantity)
                );
                self.last_open_cancel_reason = None;
                self.alive_flag = false;
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
                order.set_end_time(order_update.event_time());
                self.alive_flag = false;
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
                self.alive_flag = false;
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
            }
        });
        drop(order_manager);
        if !updated {
            warn!(
                "ArbOpenStrategy: strategy_id={} order update not found client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        let cumulative_base_qty = effective_cumulative_filled_qty * qty_multiplier;
        let base_qty_delta =
            (effective_cumulative_filled_qty - prev_cumulative_filled_qty) * qty_multiplier;
        if cumulative_base_qty > self.cumulative_open_qty + 1e-12 {
            self.cumulative_open_qty = cumulative_base_qty;
        }
        if base_qty_delta > 1e-12 {
            self.record_arb_open_fill_delta(
                order_side,
                base_qty_delta,
                order_update.event_time(),
                order_price,
                &order_update.debug_summary(),
            );
        }

        if order_update.status() == OrderStatus::New {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_new_order(order_update, &order, prev_cumulative_filled_qty);
            }
        }
        if matches!(
            order_update.status(),
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
        ) {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_terminal_order(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }
        if matches!(
            order_update.status(),
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) {
            if let Some(order) = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
            {
                self.publish_uniform_trade_order_from_order_update(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }

        if matches!(
            order_update.status(),
            OrderStatus::Canceled | OrderStatus::Filled
        ) {
            debug!(
                "ArbOpenStrategy: strategy_id={} terminal open update cumulative_open_qty={:.8} detail={}",
                self.strategy_id,
                self.cumulative_open_qty,
                order_update.debug_summary()
            );
        }

        true
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_order_id {
            debug!(
                "ArbOpenStrategy: strategy_id={} ignore trade_update client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }

        let Some(status @ (OrderStatus::PartiallyFilled | OrderStatus::Filled)) =
            trade.order_status()
        else {
            return false;
        };

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "ArbOpenStrategy: strategy_id={} trade update order missing client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };
        if OrderManager::should_skip_idempotent_trade_update(
            &current_order,
            status,
            trade.cumulative_filled_quantity(),
            trade.event_time(),
            "ArbOpenStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let qty_multiplier = current_order.qty_multiplier;
        let order_side = current_order.side;
        let cumulative_qty = trade.cumulative_filled_quantity();
        let trade_time = trade.trade_time();
        let event_time = trade.event_time();
        let updated = order_manager.update(client_order_id, |order| {
            order.cumulative_filled_quantity = cumulative_qty;
            order.set_filled_time(trade_time);
            order.set_exchange_order_id(trade.order_id());
            if status == OrderStatus::Filled {
                order.status = OrderExecutionStatus::Filled;
                order.set_end_time(event_time);
            } else if !order.status.is_terminal() {
                order.status = OrderExecutionStatus::Create;
            }
        });
        if !updated {
            warn!(
                "ArbOpenStrategy: strategy_id={} trade update order missing client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        }
        if let Some(order) = order_manager.get(client_order_id) {
            self.publish_uniform_trade_order(trade, &order, prev_cumulative_filled_qty, status);
        }
        drop(order_manager);

        let cumulative_base_qty = cumulative_qty * qty_multiplier;
        let base_qty_delta = (cumulative_qty - prev_cumulative_filled_qty) * qty_multiplier;
        if cumulative_base_qty > self.cumulative_open_qty + 1e-12 {
            self.cumulative_open_qty = cumulative_base_qty;
        }
        if base_qty_delta > 1e-12 {
            self.record_arb_open_fill_delta(
                order_side,
                base_qty_delta,
                event_time,
                trade.price(),
                &trade.debug_summary(),
            );
        }

        if status == OrderStatus::Filled {
            debug!(
                "✅ ArbOpen订单成交完成: strategy_id={} client_order_id={} symbol={} cumulative_open_qty={:.8} detail={}",
                self.strategy_id,
                client_order_id,
                trade.symbol(),
                self.cumulative_open_qty,
                trade.debug_summary()
            );
            self.alive_flag = false;
        }

        true
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_arb_open_signal(ctx),
                Err(err) => {
                    warn!(
                        "ArbOpenStrategy: strategy_id={} decode ArbOpen failed: {}",
                        self.strategy_id, err
                    );
                    self.alive_flag = false;
                }
            },
            SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_arb_cancel_signal(ctx),
                Err(err) => warn!(
                    "ArbOpenStrategy: strategy_id={} decode ArbCancel failed: {}",
                    self.strategy_id, err
                ),
            },
            _ => {
                debug!(
                    "ArbOpenStrategy: strategy_id={} ignore signal {:?}",
                    self.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl ForceCloseControl for ArbOpenStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for ArbOpenStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn symbol(&self) -> Option<&str> {
        if self.open_symbol.is_empty() {
            None
        } else {
            Some(&self.open_symbol)
        }
    }

    fn arb_open_price_map_entry(&self) -> Option<ArbOpenPriceMapEntry> {
        Some(ArbOpenPriceMapEntry {
            symbol: self.open_symbol.clone(),
            client_order_id: self.open_order_id,
            price_qv: self.open_price_qv.into(),
        })
        .filter(|entry| !entry.symbol.is_empty() && entry.client_order_id != 0)
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        ArbOpenStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = ArbOpenStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = ArbOpenStrategy::apply_trade_update(self, trade);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        if self.try_apply_ws_order_update(response) {
            return;
        }
        if response.is_request_success() {
            return;
        }

        let client_order_id = response.client_order_id();
        if client_order_id != self.open_order_id {
            return;
        }
        let exchange = response.exchange_enum();
        let code_desc = exchange
            .and_then(|ex| describe_trade_error_code(ex, response.error_code()))
            .unwrap_or("unknown");

        match response.request_kind() {
            TradeRequestKind::Open => {
                warn!(
                    "ArbOpenStrategy: strategy_id={} open_failed: req_type={} status={} code={}({}) client_order_id={}",
                    self.strategy_id,
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
                self.handle_open_failed_cleanup(client_order_id);
            }
            TradeRequestKind::Cancel => {
                let reason = if response.is_cancel_not_cancellable() {
                    PendingOrderQueryReason::CancelRejected
                } else {
                    PendingOrderQueryReason::CancelWatchdog
                };
                warn!(
                    "ArbOpenStrategy: strategy_id={} cancel_failed: req_type={} status={} code={}({}) client_order_id={} query_reason={:?}",
                    self.strategy_id,
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id,
                    reason
                );
                self.clear_query_watchdogs(client_order_id);
                if !self.send_order_query(client_order_id, reason) {
                    let marker = if response.is_cancel_not_cancellable() {
                        "cancel rejected query send failed"
                    } else {
                        "cancel failed query send failed"
                    };
                    self.handoff_open_order_after_query_failure(client_order_id, marker);
                }
            }
            TradeRequestKind::Other => warn!(
                "ArbOpenStrategy: strategy_id={} other_failed(TODO): req_type={} status={} code={}({}) client_order_id={}",
                self.strategy_id,
                response.req_type(),
                response.status(),
                response.error_code(),
                code_desc,
                client_order_id
            ),
        }
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        self.handle_open_leg_timeout();
        self.handle_query_watchdogs();
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}

impl Drop for ArbOpenStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}

#[cfg(test)]
mod tests {
    use super::{ArbOpenStrategy, PendingOrderQueryReason, QueryWatchdog};
    use crate::strategy::order_reconcile::monotonic_cumulative_fill;

    #[test]
    fn handle_open_failed_cleanup_clears_local_strategy_state() {
        let mut strategy = ArbOpenStrategy::new(7);
        strategy.open_order_id = 7_i64 << 32 | 1;
        strategy.pending_order_query = Some(PendingOrderQueryReason::OrderWatchdog);
        strategy.order_query_watchdog = Some(QueryWatchdog {
            client_order_id: strategy.open_order_id,
            due_ts_us: 10,
            reason: PendingOrderQueryReason::OrderWatchdog,
        });
        strategy.cancel_query_watchdog = Some(QueryWatchdog {
            client_order_id: strategy.open_order_id,
            due_ts_us: 20,
            reason: PendingOrderQueryReason::CancelRejected,
        });

        strategy.handle_open_failed_cleanup(strategy.open_order_id);

        assert!(!strategy.alive_flag);
        assert!(strategy.pending_order_query.is_none());
        assert!(strategy.order_query_watchdog.is_none());
        assert!(strategy.cancel_query_watchdog.is_none());
    }

    #[test]
    fn monotonic_cumulative_fill_keeps_local_value_on_rollback() {
        assert!((monotonic_cumulative_fill(4.2, 0.0) - 4.2).abs() < 1e-12);
    }
}
