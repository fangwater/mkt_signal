use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, Side};
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::signal::cancel_signal::MmCancelCtx;
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::open_signal::MmOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::OpenPriceMapEntry;
use crate::strategy::manager::{ForceCloseControl, OrphanStrategyRole, Strategy};
use crate::strategy::open_strategy_common::{
    OpenSignalInput, OpenStrategyCommon, OpenStrategyState, PendingOrderQueryReason,
};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformAmountSource,
};
use crate::strategy::ws_order_update::try_apply_ws_order_update_for_strategy;
use log::{debug, error, info, warn};
use std::any::Any;

/// 做市开仓策略：仅处理开仓，不涉及对冲/强平/模式切换
pub struct MarketMakerOpenStrategy {
    open_state: OpenStrategyState,
}

impl MarketMakerOpenStrategy {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            open_state: OpenStrategyState::new(strategy_id),
        }
    }

    fn handle_mm_open_signal(&mut self, ctx: MmOpenCtx) {
        self.handle_open_signal_common(OpenSignalInput {
            signal_kind: "MMOpen",
            order_log_name: "MM开仓",
            order_rate_bucket: OrderRateBucket::MmOpen,
            opening_symbol: ctx.get_opening_symbol(),
            venue_u8: ctx.opening_leg.venue,
            side_u8: ctx.side,
            order_type_u8: ctx.order_type,
            qty: ctx.amount_value(),
            price: ctx.price_value(),
            price_count: ctx.price_count(),
            amount_count: ctx.amount_count(),
            exp_time: ctx.exp_time,
            create_ts: ctx.create_ts,
            from_key_len: ctx.from_key_len,
            from_key: ctx.from_key,
            price_qv: ctx.price_qv,
            price_offset: ctx.price_offset,
            close_ts: 0,
        });
    }

    fn handle_open_leg_timeout(&mut self) {
        let Some(expire_ts) = self.open_state.order.open_expire_ts else {
            return;
        };
        let now = get_timestamp_us();
        if now < expire_ts || !self.open_state.alive || self.open_state.order.open_order_id == 0 {
            return;
        }

        info!(
            "MarketMakerOpenStrategy: strategy_id={} 开仓订单超时，直接撤单 order_id={}",
            self.open_state.strategy_id, self.open_state.order.open_order_id
        );
        self.open_state.order.last_open_cancel_reason = Some("timeout");

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_state.order.open_order_id);
        if let Some(order) = order {
            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        info!(
                            "MarketMakerOpenStrategy: strategy_id={} exchange={} 发送开仓撤单请求失败: {}",
                            self.open_state.strategy_id, exchange, e
                        );
                    } else {
                        info!(
                            "MarketMakerOpenStrategy: strategy_id={} exchange={} reason=timeout 已发送开仓撤单请求 order_id={}",
                            self.open_state.strategy_id, exchange, self.open_state.order.open_order_id
                        );
                        self.open_state.order.open_expire_ts = None;
                        self.schedule_cancel_query_watchdog(self.open_state.order.open_order_id);
                    }
                }
                Err(e) => {
                    error!(
                        "MarketMakerOpenStrategy: strategy_id={} 获取开仓撤单请求字节失败: {}",
                        self.open_state.strategy_id, e
                    );
                }
            }
        }
    }

    fn handle_mm_cancel_signal(&mut self, ctx: MmCancelCtx) {
        let precise_target = ctx.strategy_id > 0;
        let from_key_preview = Self::preview_text(&String::from_utf8_lossy(&ctx.from_key), 160);
        if self.open_state.order.last_cancel_trigger_ts == Some(ctx.trigger_ts) {
            debug!(
                "MarketMakerOpenStrategy: strategy_id={} skip duplicate MMCancel trigger_ts={} open_order_id={} from_key='{}'",
                self.open_state.strategy_id,
                ctx.trigger_ts,
                self.open_state.order.open_order_id,
                from_key_preview
            );
            return;
        }
        if precise_target {
            if ctx.strategy_id != self.open_state.strategy_id {
                info!(
                    "MarketMakerOpenStrategy: strategy_id={} ignore targeted MMCancel target_strategy_id={} trigger_ts={} from_key='{}'",
                    self.open_state.strategy_id,
                    ctx.strategy_id,
                    ctx.trigger_ts,
                    from_key_preview
                );
                return;
            }
        } else {
            if ctx.client_order_id > 0 && ctx.client_order_id != self.open_state.order.open_order_id
            {
                info!(
                    "MarketMakerOpenStrategy: strategy_id={} ignore MMCancel due to client_order_id mismatch signal_client_order_id={} open_order_id={} trigger_ts={} from_key='{}'",
                    self.open_state.strategy_id,
                    ctx.client_order_id,
                    self.open_state.order.open_order_id,
                    ctx.trigger_ts,
                    from_key_preview
                );
                return;
            }

            let cancel_side = ctx.get_side();
            if let Some(open_side) = self.open_state.order.open_side {
                if open_side != cancel_side {
                    info!(
                        "MarketMakerOpenStrategy: strategy_id={} skip MMCancel due to side mismatch open_side={:?} cancel_side={:?} open_order_id={} trigger_ts={} from_key='{}'",
                        self.open_state.strategy_id,
                        open_side,
                        cancel_side,
                        self.open_state.order.open_order_id,
                        ctx.trigger_ts,
                        from_key_preview
                    );
                    return;
                }
            }
        }

        if self.open_state.order.pending_order_query.is_some()
            || self
                .open_state
                .order
                .cancel_query_watchdog
                .is_some_and(|w| w.client_order_id == self.open_state.order.open_order_id)
        {
            debug!(
                "MarketMakerOpenStrategy: strategy_id={} skip MMCancel because cancel reconcile already in flight open_order_id={} trigger_ts={} from_key='{}'",
                self.open_state.strategy_id,
                self.open_state.order.open_order_id,
                ctx.trigger_ts,
                from_key_preview
            );
            self.open_state.order.last_cancel_trigger_ts = Some(ctx.trigger_ts);
            return;
        }
        if self.open_state.order.open_order_id == 0 {
            info!(
                "MarketMakerOpenStrategy: strategy_id={} skip MMCancel because open_order_id=0 trigger_ts={} from_key='{}'",
                self.open_state.strategy_id,
                ctx.trigger_ts,
                from_key_preview
            );
            return;
        }

        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(self.open_state.order.open_order_id);
        if let Some(order) = order {
            if order.status.is_terminal() {
                info!(
                    "MarketMakerOpenStrategy: strategy_id={} open order already terminal {:?}, skip cancel order_id={} trigger_ts={} from_key='{}'",
                    self.open_state.strategy_id, order.status, self.open_state.order.open_order_id, ctx.trigger_ts, from_key_preview
                );
                return;
            }

            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    let exchange = order.venue.trade_engine_exchange();
                    let cancel_reason = ctx.get_reason().as_log_reason();
                    if let Err(e) = TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
                        error!(
                            "MarketMakerOpenStrategy: strategy_id={} exchange={} 发送撤单请求失败 order_id={} trigger_ts={} from_key='{}' err={}",
                            self.open_state.strategy_id, exchange, self.open_state.order.open_order_id, ctx.trigger_ts, from_key_preview, e
                        );
                    } else {
                        self.open_state.order.last_open_cancel_reason = Some(cancel_reason);
                        self.open_state.order.last_cancel_trigger_ts = Some(ctx.trigger_ts);
                        self.schedule_cancel_query_watchdog(order.client_order_id);
                        info!(
                            "MarketMakerOpenStrategy: strategy_id={} exchange={} reason={} 已发送开仓撤单请求 order_id={} trigger_ts={} from_key='{}'",
                            self.open_state.strategy_id,
                            exchange,
                            cancel_reason,
                            self.open_state.order.open_order_id,
                            ctx.trigger_ts,
                            from_key_preview
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "MarketMakerOpenStrategy: strategy_id={} 获取撤单请求字节失败 order_id={} trigger_ts={} from_key='{}' err={}",
                        self.open_state.strategy_id, self.open_state.order.open_order_id, ctx.trigger_ts, from_key_preview, e
                    );
                }
            }
        } else {
            info!(
                "MarketMakerOpenStrategy: strategy_id={} 未找到要撤销的订单 order_id={} trigger_ts={} from_key='{}'",
                self.open_state.strategy_id, self.open_state.order.open_order_id, ctx.trigger_ts, from_key_preview
            );
        }
    }

    fn record_open_order_terminal(
        &mut self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        terminal_qty: f64,
        fill_ts: i64,
        price: f64,
        update_detail: &str,
    ) {
        if terminal_qty <= 1e-12 {
            return;
        }
        let base_qty = MonitorChannel::instance().qty_to_base(venue, symbol, terminal_qty);
        if base_qty <= 0.0 {
            return;
        }
        let symbol_internal = normalize_symbol_for_internal(symbol);
        let signed_base_qty = match side {
            Side::Buy => base_qty,
            Side::Sell => -base_qty,
        };

        let strategy_mgr = MonitorChannel::instance().strategy_mgr();
        let updated = strategy_mgr.borrow_mut().record_open_order_terminal(
            &symbol_internal,
            signed_base_qty,
            fill_ts,
            price,
            0,
        );
        if !updated {
            let hedge_symbols = {
                let snapshots = strategy_mgr.borrow().mm_hedge_snapshots();
                let preview: Vec<String> = snapshots
                    .into_iter()
                    .take(8)
                    .map(|snap| {
                        format!(
                            "{}(net={:.8},buy={:.8},sell={:.8})",
                            snap.symbol, snap.net_qty, snap.buy_qty, snap.sell_qty
                        )
                    })
                    .collect();
                if preview.is_empty() {
                    "-".to_string()
                } else {
                    preview.join(",")
                }
            };
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} record open order terminal failed venue={:?} raw_symbol={} internal_symbol={} side={:?} terminal_qty={:.8} base_qty={:.8} fill_ts={} price={:.8} open_order_id={} update={} mm_hedge_snapshots={}",
                self.open_state.strategy_id,
                venue,
                symbol,
                symbol_internal,
                side,
                terminal_qty,
                base_qty,
                fill_ts,
                price,
                self.open_state.order.open_order_id,
                update_detail,
                hedge_symbols
            );
        }
    }

    fn create_and_send_order(&mut self, client_order_id: i64, symbol: &str) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id);
        let Some(order) = order else {
            self.open_state.alive = false;
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
                    "MarketMakerOpenStrategy: strategy_id={} MM open order action recorded client_order_id={} count_10s={} count_1m={}",
                    self.open_state.strategy_id, client_order_id, stats.count_10s, stats.count_1m
                );
                if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                    self.open_state.alive = false;
                    return Err(format!(
                        "publish order request failed: symbol={} exchange={} err={}",
                        symbol, exchange, e
                    ));
                }
                self.schedule_order_query_watchdog(client_order_id);
                Ok(())
            }
            Err(e) => {
                self.open_state.alive = false;
                Err(format!("get order request bytes failed: {}", e))
            }
        }
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        let client_order_id = order_update.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_state.order.open_order_id {
            debug!(
                "MarketMakerOpenStrategy: strategy_id={} ignore order_update client_order_id={}",
                self.open_state.strategy_id, client_order_id
            );
            return false;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} order update not found client_order_id={}",
                self.open_state.strategy_id, client_order_id
            );
            return false;
        };

        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            "MarketMakerOpenStrategy",
            self.open_state.strategy_id,
        )
        .is_some()
        {
            return false;
        }

        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let prev_order_terminal = current_order.status.is_terminal();
        let incoming_cumulative_filled_qty = order_update.cumulative_filled_quantity();
        let protected_cumulative_fill =
            current_order.protected_cumulative_fill(incoming_cumulative_filled_qty);
        if protected_cumulative_fill.rollback_detected {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} protected cumulative fill rollback: client_order_id={} symbol={} status={:?} prev={:.8} incoming={:.8} effective={:.8}",
                self.open_state.strategy_id,
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
                if !self.open_state.alive {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} revive on delayed open NEW: client_order_id={} exchange_order_id={} symbol={}",
                        self.open_state.strategy_id,
                        client_order_id,
                        order_update.order_id(),
                        order.symbol
                    );
                    self.open_state.alive = true;
                }
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
                debug!(
                    "✅ MM订单已挂单: strategy_id={} client_order_id={} exchange_order_id={} symbol={} side={:?} price={:.6} qty={:.4}",
                    self.open_state.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol,
                    order.side,
                    order.price,
                    order.quantity
                );
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
                let cancel_reason = self.open_state.order.last_open_cancel_reason.unwrap_or("unknown");
                info!(
                    "🚫 MM订单已撤销: strategy_id={} client_order_id={} exchange_order_id={} exchange={} symbol={} reason={} side={:?} price={:.6} qty={:.4} filled={:.4}/{:.4}",
                    self.open_state.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.venue.trade_engine_exchange(),
                    order.symbol,
                    cancel_reason,
                    order.side,
                    order.price,
                    order.quantity,
                    order.cumulative_filled_quantity,
                    order.quantity
                );
                self.open_state.order.last_open_cancel_reason = None;
                self.open_state.alive = false;
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
                order.set_end_time(order_update.event_time());
                debug!(
                    "✅ MM订单已完全成交: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.open_state.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
                self.open_state.alive = false;
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
                warn!(
                    "⏰ MM订单已过期: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.open_state.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
                self.open_state.alive = false;
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_filled_time(order_update.event_time());
                debug!(
                    "MM订单部分成交: strategy_id={} client_order_id={} exchange_order_id={} symbol={}",
                    self.open_state.strategy_id,
                    client_order_id,
                    order_update.order_id(),
                    order.symbol
                );
            }
        });
        drop(order_manager);

        if !updated {
            warn!(
                "MarketMakerOpenStrategy: strategy_id={} order update not found client_order_id={}",
                self.open_state.strategy_id, client_order_id
            );
            return false;
        }

        let mut record_fill: Option<(TradingVenue, String, Side, f64, f64, String)> = None;
        if matches!(
            order_update.status(),
            OrderStatus::Canceled | OrderStatus::Filled
        ) {
            let terminal_qty = if prev_order_terminal {
                effective_cumulative_filled_qty - prev_cumulative_filled_qty
            } else {
                effective_cumulative_filled_qty
            };
            let update_detail = order_update.debug_summary();
            record_fill = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id)
                .map(|order| {
                    (
                        order.venue,
                        order.symbol.clone(),
                        order.side,
                        terminal_qty,
                        order.price,
                        format!(
                            "{} local_order_symbol={} local_order_qty={:.8} local_order_status={:?}",
                            update_detail, order.symbol, order.quantity, order.status
                        ),
                    )
                });
        }

        if let Some((venue, symbol, side, qty, price, update_detail)) = record_fill {
            self.record_open_order_terminal(
                venue,
                &symbol,
                side,
                qty,
                order_update.event_time(),
                price,
                &update_detail,
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

        true
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_uniform_new_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "MarketMakerOpenStrategy",
            self.open_state.strategy_id,
            UniformAmountSource::LocalOrder,
        );
    }

    fn publish_uniform_terminal_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_uniform_terminal_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "MarketMakerOpenStrategy",
            self.open_state.strategy_id,
            UniformAmountSource::LocalOrder,
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
        publish_uniform_trade_order(
            trade,
            order,
            prev_cumulative_filled_qty,
            status,
            &ctx,
            "MarketMakerOpenStrategy",
            self.open_state.strategy_id,
        );
    }

    fn publish_uniform_trade_order_from_order_update(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_open_publish_ctx();
        publish_uniform_trade_order_from_order_update(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "MarketMakerOpenStrategy",
            self.open_state.strategy_id,
        );
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        self.clear_query_watchdogs(client_order_id);
        if client_order_id != self.open_state.order.open_order_id {
            debug!(
                "MarketMakerOpenStrategy: strategy_id={} ignore trade_update client_order_id={}",
                self.open_state.strategy_id, client_order_id
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
                "MarketMakerOpenStrategy: strategy_id={} trade update order missing client_order_id={}",
                self.open_state.strategy_id, client_order_id
            );
            return false;
        };
        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let prev_order_terminal = current_order.status.is_terminal();
        if OrderManager::should_skip_idempotent_trade_update(
            &current_order,
            status,
            trade.cumulative_filled_quantity(),
            trade.event_time(),
            "MarketMakerOpenStrategy",
            self.open_state.strategy_id,
        )
        .is_some()
        {
            return false;
        }

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
                "MarketMakerOpenStrategy: strategy_id={} trade update order missing client_order_id={}",
                self.open_state.strategy_id, client_order_id
            );
            return false;
        }

        let order_snapshot = order_manager.get(client_order_id).map(|order| {
            (
                order.venue,
                order.symbol.clone(),
                order.side,
                order.price,
                format!(
                    "{} local_order_symbol={} local_order_qty={:.8} local_order_status={:?}",
                    trade.debug_summary(),
                    order.symbol,
                    order.quantity,
                    order.status
                ),
            )
        });
        if let Some(order) = order_manager.get(client_order_id) {
            self.publish_uniform_trade_order(trade, &order, prev_cumulative_filled_qty, status);
        }
        drop(order_manager);

        if status == OrderStatus::Filled {
            let terminal_qty = if prev_order_terminal {
                cumulative_qty - prev_cumulative_filled_qty
            } else {
                cumulative_qty
            };
            let order_price = order_snapshot
                .as_ref()
                .map(|(_, _, _, price, _)| *price)
                .unwrap_or_else(|| trade.price());
            let (record_venue, record_symbol, record_side, record_detail) = order_snapshot
                .as_ref()
                .map(|(venue, symbol, side, _, detail)| {
                    (*venue, symbol.as_str(), *side, detail.as_str())
                })
                .unwrap_or_else(|| (trade.trading_venue(), trade.symbol(), trade.side(), "-"));
            debug!(
                "✅ MM订单成交完成: strategy_id={} client_order_id={} symbol={} price={:.6} cumulative={:.4}",
                self.open_state.strategy_id,
                client_order_id,
                trade.symbol(),
                trade.price(),
                cumulative_qty
            );
            self.record_open_order_terminal(
                record_venue,
                record_symbol,
                record_side,
                terminal_qty,
                event_time,
                order_price,
                record_detail,
            );
            self.open_state.alive = false;
        }

        true
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::MMOpen => match MmOpenCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_mm_open_signal(ctx),
                Err(err) => {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} decode MMOpen failed: {}",
                        self.open_state.strategy_id, err
                    );
                    self.open_state.alive = false;
                }
            },
            SignalType::MMCancel => match MmCancelCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_mm_cancel_signal(ctx),
                Err(err) => {
                    warn!(
                        "MarketMakerOpenStrategy: strategy_id={} decode MMCancel failed: {}",
                        self.open_state.strategy_id, err
                    );
                }
            },
            _ => {
                debug!(
                    "MarketMakerOpenStrategy: strategy_id={} ignore signal {:?}",
                    self.open_state.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl OpenStrategyCommon for MarketMakerOpenStrategy {
    fn strategy_name(&self) -> &'static str {
        "MarketMakerOpenStrategy"
    }

    fn open_state(&self) -> &OpenStrategyState {
        &self.open_state
    }

    fn open_state_mut(&mut self) -> &mut OpenStrategyState {
        &mut self.open_state
    }

    fn orphan_strategy_role(&self) -> OrphanStrategyRole {
        OrphanStrategyRole::Mm
    }

    fn open_order_qty_to_base(
        &self,
        venue: TradingVenue,
        symbol: &str,
        signed_qty: f64,
        _qty_multiplier: f64,
    ) -> Result<f64, String> {
        Ok(MonitorChannel::instance().qty_to_base(venue, symbol, signed_qty))
    }

    fn create_and_send_open_order(
        &mut self,
        client_order_id: i64,
        symbol: &str,
    ) -> Result<(), String> {
        self.create_and_send_order(client_order_id, symbol)
    }

    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    ) {
        warn!(
            "MarketMakerOpenStrategy: strategy_id={} order query {} failed, handoff to mm orphan: client_order_id={}",
            self.open_state.strategy_id, marker, client_order_id
        );
        self.handoff_open_order_to_orphan(client_order_id, marker);
    }
}

impl ForceCloseControl for MarketMakerOpenStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for MarketMakerOpenStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id()
    }

    fn symbol(&self) -> Option<&str> {
        self.open_strategy_symbol()
    }

    fn mm_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
        self.open_price_map_entry()
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.open_state.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        MarketMakerOpenStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = MarketMakerOpenStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = MarketMakerOpenStrategy::apply_trade_update(self, trade);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        if try_apply_ws_order_update_for_strategy(self, response) {
            return;
        }

        if response.is_request_success() {
            return;
        }

        let client_order_id = response.client_order_id();
        if client_order_id != self.open_state.order.open_order_id {
            return;
        }

        let exchange = response.exchange_enum();
        let code_desc = exchange
            .and_then(|ex| describe_trade_error_code(ex, response.error_code()))
            .unwrap_or("unknown");

        match response.request_kind() {
            TradeRequestKind::Open => {
                warn!(
                    "MarketMakerOpenStrategy: strategy_id={} open_failed: req_type={} status={} code={}({}) client_order_id={}",
                    self.open_state.strategy_id,
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
                    "MarketMakerOpenStrategy: strategy_id={} cancel_failed: req_type={} status={} code={}({}) client_order_id={} query_reason={:?}",
                    self.open_state.strategy_id,
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
            TradeRequestKind::Other => {
                warn!(
                    "MarketMakerOpenStrategy: strategy_id={} other_failed(TODO): req_type={} status={} code={}({}) client_order_id={}",
                    self.open_state.strategy_id,
                    response.req_type(),
                    response.status(),
                    response.error_code(),
                    code_desc,
                    client_order_id
                );
            }
        }
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        self.handle_open_leg_timeout();
        self.handle_query_watchdogs();
    }

    fn is_active(&self) -> bool {
        self.open_strategy_is_active()
    }
}

#[cfg(test)]
mod tests {
    use super::MarketMakerOpenStrategy;
    use crate::strategy::open_strategy_common::{
        OpenStrategyCommon, PendingOrderQueryReason, QueryWatchdog,
    };
    use crate::strategy::order_reconcile::monotonic_cumulative_fill;

    #[test]
    fn handle_open_failed_cleanup_clears_local_strategy_state() {
        let mut strategy = MarketMakerOpenStrategy::new(7);
        strategy.open_state.order.open_order_id = 7_i64 << 32 | 1;
        strategy.open_state.order.pending_order_query =
            Some(PendingOrderQueryReason::OrderWatchdog);
        strategy.open_state.order.order_query_watchdog = Some(QueryWatchdog {
            client_order_id: strategy.open_state.order.open_order_id,
            due_ts_us: 10,
            reason: PendingOrderQueryReason::OrderWatchdog,
        });
        strategy.open_state.order.cancel_query_watchdog = Some(QueryWatchdog {
            client_order_id: strategy.open_state.order.open_order_id,
            due_ts_us: 20,
            reason: PendingOrderQueryReason::CancelRejected,
        });

        strategy.handle_open_failed_cleanup(strategy.open_state.order.open_order_id);

        assert!(!strategy.open_state.alive);
        assert!(strategy.open_state.order.pending_order_query.is_none());
        assert!(strategy.open_state.order.order_query_watchdog.is_none());
        assert!(strategy.open_state.order.cancel_query_watchdog.is_none());
    }

    #[test]
    fn monotonic_cumulative_fill_keeps_local_value_on_rollback() {
        assert!((monotonic_cumulative_fill(4.2, 0.0) - 4.2).abs() < 1e-12);
    }

    #[test]
    fn monotonic_cumulative_fill_accepts_forward_progress() {
        assert!((monotonic_cumulative_fill(4.2, 5.6) - 5.6).abs() < 1e-12);
    }
}

impl Drop for MarketMakerOpenStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}
