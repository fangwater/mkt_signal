use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::PersistChannel;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::{OpenPriceMapEntry, OrphanStrategyRole, Strategy};
use crate::strategy::open_strategy_common::{
    OpenCancelInput, OpenSignalInput, OpenStrategyCommon, OpenStrategyState,
};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::trade_update_lite::TradeUpdateLite;
use crate::strategy::uniform_order_helper::UniformPublishCtx;
use log::{debug, warn};
use std::any::Any;

/// 单腿套利开仓策略：只负责 open leg 生命周期，不保存 hedge leg 或双腿盘口。
pub struct ArbOpenStrategy {
    open_state: OpenStrategyState,
    pub close_ts: Option<i64>, //仓位希望持有的时间
    // TODO: apply_trade_update_lite 会使用这些 open leg 观测字段。
    pub cumulative_open_qty: f64, //累计开仓数量
    pub open_qty_multiplier: f64, //开仓侧数量乘数（venue qty -> base qty）
}

impl ArbOpenStrategy {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            open_state: OpenStrategyState::new(strategy_id),
            close_ts: None,
            cumulative_open_qty: 0.0,
            open_qty_multiplier: 1.0,
        }
    }

    fn handle_arb_open_signal(&mut self, ctx: ArbOpenCtx) {
        let close_ts = if ctx.hedge_timeout_us > 0 {
            let base_ts = if ctx.create_ts > 0 {
                ctx.create_ts
            } else {
                get_timestamp_us()
            };
            base_ts.saturating_add(ctx.hedge_timeout_us)
        } else {
            0
        };

        let symbol = ctx.get_opening_symbol();
        if let Some(venue) = TradingVenue::from_u8(ctx.opening_leg.venue) {
            MonitorChannel::instance().seed_close_inventory_if_absent(venue, &symbol);
        }

        let mkt_ts = ctx.opening_leg.ts.max(ctx.hedging_leg.ts);
        let Some(init) = self.handle_open_signal_common(OpenSignalInput {
            signal_kind: "ArbOpen",
            order_log_name: "ArbOpen",
            order_rate_bucket: OrderRateBucket::ArbOpen,
            opening_symbol: symbol,
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
            reduce_only: false,
            client_order_id: None,
            close_ts,
            mkt_ts,
            signal_type_u8: SignalType::ArbOpen as u8,
        }) else {
            return;
        };
        self.close_ts = if init.close_ts > 0 {
            Some(init.close_ts)
        } else {
            None
        };
        self.cumulative_open_qty = 0.0;
        self.open_qty_multiplier = init.qty_multiplier;
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        self.apply_order_update_common(order_update)
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        self.apply_trade_update_common(trade)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_arb_open_signal(ctx),
                Err(err) => {
                    warn!(
                        "ArbOpenStrategy: strategy_id={} decode ArbOpen failed: {}",
                        self.open_state.strategy_id, err
                    );
                    self.mark_open_strategy_inactive(format!("decode ArbOpen failed: {}", err));
                }
            },
            SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => {
                    let mkt_ts = ctx.opening_leg.ts.max(ctx.hedging_leg.ts);
                    self.handle_open_cancel_signal_common(OpenCancelInput {
                        signal_name: "ArbCancel",
                        target_strategy_id: ctx.strategy_id,
                        target_client_order_id: 0,
                        cancel_side: ctx.get_side(),
                        cancel_reason: ctx.get_reason().as_log_reason(),
                        trigger_ts: ctx.trigger_ts,
                        from_key: ctx.from_key,
                        mkt_ts,
                    })
                }
                Err(err) => warn!(
                    "ArbOpenStrategy: strategy_id={} decode ArbCancel failed: {}",
                    self.open_state.strategy_id, err
                ),
            },
            _ => {
                debug!(
                    "ArbOpenStrategy: strategy_id={} ignore signal {:?}",
                    self.open_state.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl OpenStrategyCommon for ArbOpenStrategy {
    fn strategy_name(&self) -> &'static str {
        "ArbOpenStrategy"
    }

    fn cancel_signal_type_u8(&self) -> u8 {
        SignalType::ArbCancel as u8
    }

    fn open_state(&self) -> &OpenStrategyState {
        &self.open_state
    }

    fn open_state_mut(&mut self) -> &mut OpenStrategyState {
        &mut self.open_state
    }

    fn open_order_non_terminal_cleanup_reason(&self) -> &'static str {
        "ArbOpen开仓订单未达到终结状态被清理"
    }

    fn orphan_strategy_role(&self) -> OrphanStrategyRole {
        OrphanStrategyRole::Arb
    }

    fn open_order_rate_bucket(&self) -> OrderRateBucket {
        OrderRateBucket::ArbOpen
    }

    fn open_order_action_log_name(&self) -> &'static str {
        "arb open"
    }

    fn open_terminal_close_ts(&self) -> i64 {
        self.close_ts.unwrap_or(0)
    }

    fn log_open_deleveraging_risk_rejects(&self) -> bool {
        true
    }

    fn update_close_inventory_for_open_fill(
        &self,
        venue: TradingVenue,
        symbol: &str,
        side: crate::pre_trade::order_manager::Side,
        filled_base_delta: f64,
    ) {
        MonitorChannel::instance().apply_open_inventory_fill_delta(
            venue,
            symbol,
            side,
            filled_base_delta,
        );
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
            "ArbOpenStrategy: strategy_id={} order query {} failed, handoff to hedge orphan: client_order_id={}",
            self.open_state.strategy_id, marker, client_order_id
        );
        self.handoff_open_order_to_orphan(client_order_id, marker);
    }

    /// arb open 用自身 client_order_id 作为 uniform from_key；
    /// 配对的 hedge 单回写时也用同一个 id —— 下游可直接按 from_key 字符串 JOIN open/hedge。
    fn uniform_open_publish_ctx(&self) -> UniformPublishCtx {
        let open_state = self.open_state();
        UniformPublishCtx {
            signal_ts: open_state.signal_ts,
            from_key: open_state.order.open_order_id.to_string().into_bytes(),
            price_offset: open_state.price_offset,
        }
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
        self.strategy_id()
    }

    fn symbol(&self) -> Option<&str> {
        self.open_strategy_symbol()
    }

    fn arb_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
        self.open_price_map_entry()
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.open_state.strategy_id
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

    fn apply_trade_update_lite(&mut self, trade: &dyn TradeUpdateLite) {
        let _ = self.apply_trade_update_lite_common(trade);
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

impl Drop for ArbOpenStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}

#[cfg(test)]
mod tests {
    use super::ArbOpenStrategy;
    use crate::strategy::open_strategy_common::{
        OpenStrategyCommon, PendingOrderQueryReason, QueryWatchdog,
    };
    use crate::strategy::order_reconcile::monotonic_cumulative_fill;

    #[test]
    fn handle_open_failed_cleanup_clears_local_strategy_state() {
        let mut strategy = ArbOpenStrategy::new(7);
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
}
