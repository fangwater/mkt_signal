use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::PersistChannel;
use crate::strategy::manager::OpenPriceMapEntry;
use crate::strategy::manager::{OrphanStrategyRole, Strategy};
use crate::strategy::open_strategy_common::{
    OpenCancelInput, OpenSignalInput, OpenStrategyCommon, OpenStrategyState,
};
use crate::strategy::uniform_order_helper::signal_bbo_from_legs;
use log::{debug, warn};
use order_common::OrderUpdate;
use order_common::TradeEngineResponse;
use order_common::TradeUpdate;
use order_common::TradingVenue;
use signal_common::cancel_signal::MmCancelCtx;
use signal_common::open_signal::MmOpenCtxView;
use signal_common::trade_signal::{SignalType, TradeSignal};
use std::any::Any;
use std::borrow::Cow;

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

    pub fn handle_mm_open_view_with_symbol(
        &mut self,
        ctx: MmOpenCtxView<'_>,
        symbol: Cow<'_, str>,
        pending_limit_prechecked: bool,
    ) {
        let _ = self.handle_open_signal_common(OpenSignalInput {
            signal_kind: "MMOpen",
            order_log_name: "MM开仓",
            order_rate_bucket: OrderRateBucket::MmOpen,
            opening_symbol: symbol,
            opening_symbol_normalized: true,
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
            from_key: Cow::Borrowed(ctx.from_key),
            signal_bbo: signal_bbo_from_legs(Some(&ctx.opening_leg), None),
            price_qv: ctx.price_qv,
            order_qty_qv: Some(ctx.amount_qv),
            order_price_qv: Some(ctx.price_qv),
            price_offset: ctx.price_offset,
            reduce_only: false,
            bitget_spot_order: false,
            client_order_id: None,
            pending_limit_prechecked,
            close_ts: 0,
            mkt_ts: ctx.opening_leg.ts,
            signal_type_u8: SignalType::MMOpen as u8,
            pre_trade_recv_ts: 0,
            pre_trade_handle_ts: 0,
        });
    }

    pub fn handle_mm_cancel_ctx(&mut self, ctx: &MmCancelCtx) {
        self.handle_open_cancel_signal_common(OpenCancelInput {
            signal_name: "MMCancel",
            target_strategy_id: ctx.strategy_id,
            target_client_order_id: ctx.client_order_id,
            cancel_side: ctx.get_side(),
            cancel_reason: ctx.get_reason().as_log_reason(),
            trigger_ts: ctx.trigger_ts,
            from_key: ctx.from_key.clone(),
            mkt_ts: ctx.opening_leg.ts,
        });
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        self.apply_order_update_common(order_update)
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        self.apply_trade_update_common(trade)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::MMOpen => warn!(
                "MarketMakerOpenStrategy: strategy_id={} unexpected MMOpen in strategy signal handler; open signals must be constructed by pre_trade fast path",
                self.open_state.strategy_id
            ),
            SignalType::MMCancel => match MmCancelCtx::from_slice(signal.context.as_ref()) {
                Ok(ctx) => self.handle_mm_cancel_ctx(&ctx),
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

    fn cancel_signal_type_u8(&self) -> u8 {
        SignalType::MMCancel as u8
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

    fn open_order_rate_bucket(&self) -> OrderRateBucket {
        OrderRateBucket::MmOpen
    }

    fn open_order_action_log_name(&self) -> &'static str {
        "MM open"
    }

    fn resolve_open_qty_multiplier(
        &self,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        MonitorChannel::instance().qty_multiplier_for_venue(venue, symbol)
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
        self.apply_trade_engine_response_common(response);
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        self.handle_open_leg_timeout_common();
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
