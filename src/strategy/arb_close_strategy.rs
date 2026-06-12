use crate::pre_trade::log_throttle::{
    log_close_below_min_trade_summary, log_pending_limit_summary,
};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::PersistChannel;
use crate::strategy::manager::{OrphanStrategyRole, Strategy};
use crate::strategy::open_strategy_common::{
    OpenSignalInput, OpenStrategyCommon, OpenStrategyState,
};
use log::{debug, info, warn};
use order_common::OrderUpdate;
use order_common::Side;
use order_common::TradeEngineResponse;
use order_common::TradeUpdate;
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use signal_common::common::SignalBytes;
use signal_common::open_signal::ArbOpenCtx;
use signal_common::trade_signal::{SignalType, TradeSignal};
use std::any::Any;

const ARB_CLOSE_QTY_EPS: f64 = 1e-12;

/// Arb close 复用 common open 下单生命周期，按信号数量逐单执行。
pub struct ArbCloseStrategy {
    open_state: OpenStrategyState,
}

impl ArbCloseStrategy {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            open_state: OpenStrategyState::new(strategy_id),
        }
    }

    pub fn close_side(&self) -> Option<Side> {
        self.open_side()
    }

    fn handle_arb_close_signal(&mut self, mut ctx: ArbOpenCtx) {
        let symbol = normalize_symbol_for_internal(&ctx.get_opening_symbol());
        if symbol.is_empty() {
            warn!(
                "ArbCloseStrategy: strategy_id={} empty opening symbol",
                self.open_state.strategy_id
            );
            self.open_state.alive = false;
            return;
        }
        let hedging_symbol = normalize_symbol_for_internal(&ctx.get_hedging_symbol());
        ctx.set_opening_symbol(&symbol);
        ctx.set_hedging_symbol(&hedging_symbol);

        let Some(venue) = TradingVenue::from_u8(ctx.opening_leg.venue) else {
            warn!(
                "ArbCloseStrategy: strategy_id={} invalid opening venue={}",
                self.open_state.strategy_id, ctx.opening_leg.venue
            );
            self.open_state.alive = false;
            return;
        };
        let Some(side) = Side::from_u8(ctx.side) else {
            warn!(
                "ArbCloseStrategy: strategy_id={} invalid close side={}",
                self.open_state.strategy_id, ctx.side
            );
            self.open_state.alive = false;
            return;
        };

        let open_pos = MonitorChannel::instance().get_position_qty(&symbol, venue);
        if ctx.amount_value() <= ARB_CLOSE_QTY_EPS || ctx.amount_count() <= 0 {
            info!(
                "ArbCloseStrategy: strategy_id={} skip because signal close qty is zero symbol={} venue={:?} open_pos={:.8} signal_qty={:.8}",
                self.open_state.strategy_id,
                symbol,
                venue,
                open_pos,
                ctx.amount_value()
            );
            self.open_state.alive = false;
            return;
        }

        if let Err(e) = MonitorChannel::instance().check_pending_limit_order_for_arb(&symbol, side)
        {
            log_pending_limit_summary(
                "ArbCloseStrategy",
                Some(self.open_state.strategy_id),
                &symbol,
                side,
                &e,
            );
            self.open_state.alive = false;
            return;
        }

        let client_order_id = Self::compose_order_id(self.open_state.strategy_id);
        let qty_multiplier = match self.resolve_open_qty_multiplier(venue, &symbol) {
            Ok(multiplier) => multiplier,
            Err(err) => {
                warn!(
                    "ArbCloseStrategy: strategy_id={} resolve qty multiplier failed symbol={} venue={:?}: {}",
                    self.open_state.strategy_id, symbol, venue, err
                );
                self.open_state.alive = false;
                return;
            }
        };
        let requested_base_qty = ctx.amount_value() * qty_multiplier;
        let grant = MonitorChannel::instance().reserve_close_inventory_silent(
            venue,
            &symbol,
            side,
            requested_base_qty,
            client_order_id,
        );
        if grant.granted_base_qty <= ARB_CLOSE_QTY_EPS {
            if !close_residual_is_effectively_done(open_pos, requested_base_qty) {
                info!(
                    "ArbCloseStrategy: strategy_id={} skip because close inventory unavailable symbol={} venue={:?} side={:?} open_pos={:.8} signal_qty={:.8} requested_base={:.8} available_before={:.8} inventory={:.8}",
                    self.open_state.strategy_id,
                    symbol,
                    venue,
                    side,
                    open_pos,
                    ctx.amount_value(),
                    requested_base_qty,
                    grant.available_before_base,
                    grant.closable_inventory_base
                );
            }
            self.open_state.alive = false;
            return;
        }
        let raw_order_qty = grant.granted_base_qty / qty_multiplier;

        // Close must never round up: topping up a dust close can open the opposite side.
        // If the capped quantity is below venue step/min, release the reservation silently.
        let (order_qty, order_price) = match MonitorChannel::instance().align_close_order_by_venue(
            venue,
            &symbol,
            raw_order_qty,
            ctx.price_value(),
        ) {
            Ok(Some(aligned)) => aligned,
            Ok(None) => {
                MonitorChannel::instance().release_close_inventory_unfilled_silent(
                    client_order_id,
                    "close_qty_below_step_or_min",
                );
                self.open_state.alive = false;
                return;
            }
            Err(reason) => {
                MonitorChannel::instance().release_close_inventory_unfilled(
                    client_order_id,
                    "close_qty_alignment_failed",
                );
                info!(
                    "ArbCloseStrategy: strategy_id={} skip close qty alignment failed symbol={} venue={:?} open_pos={:.8} signal_qty={:.8} raw_order_qty={:.12} price={:.8} reason={}",
                    self.open_state.strategy_id,
                    symbol,
                    venue,
                    open_pos,
                    ctx.amount_value(),
                    raw_order_qty,
                    ctx.price_value(),
                    reason
                );
                self.open_state.alive = false;
                return;
            }
        };
        let price_hint = Some(order_price);
        if let Err(reason) = MonitorChannel::instance()
            .check_min_trading_requirements(venue, &symbol, order_qty, price_hint)
        {
            MonitorChannel::instance().release_close_inventory_unfilled_silent(
                client_order_id,
                "below_min_trade_requirement",
            );
            log_close_below_min_trade_summary(
                "ArbCloseStrategy",
                Some(self.open_state.strategy_id),
                venue,
                &symbol,
                side,
                open_pos,
                order_qty,
                price_hint,
                &reason,
            );
            self.open_state.alive = false;
            return;
        }

        let mkt_ts = ctx.opening_leg.ts.max(ctx.hedging_leg.ts);
        let init = self.handle_open_signal_common(OpenSignalInput {
            signal_kind: "ArbClose",
            order_log_name: "ArbClose",
            order_rate_bucket: OrderRateBucket::ArbOpen,
            opening_symbol: symbol,
            venue_u8: ctx.opening_leg.venue,
            side_u8: ctx.side,
            order_type_u8: ctx.order_type,
            qty: order_qty,
            price: order_price,
            price_count: ctx.price_count(),
            amount_count: ctx.amount_count(),
            exp_time: ctx.exp_time,
            create_ts: ctx.create_ts,
            from_key_len: ctx.from_key_len,
            from_key: ctx.from_key,
            price_qv: ctx.price_qv,
            order_qty_qv: None,
            order_price_qv: None,
            price_offset: ctx.price_offset,
            reduce_only: true,
            client_order_id: Some(client_order_id),
            pending_limit_prechecked: false,
            close_ts: 0,
            mkt_ts,
            signal_type_u8: SignalType::ArbClose as u8,
        });
        if init.is_none() {
            MonitorChannel::instance()
                .release_close_inventory_unfilled(client_order_id, "handle_open_signal_failed");
        }
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::ArbClose => match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_arb_close_signal(ctx),
                Err(err) => {
                    warn!(
                        "ArbCloseStrategy: strategy_id={} decode ArbClose failed: {}",
                        self.open_state.strategy_id, err
                    );
                    self.open_state.alive = false;
                }
            },
            _ => {
                debug!(
                    "ArbCloseStrategy: strategy_id={} ignore signal {:?}",
                    self.open_state.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl OpenStrategyCommon for ArbCloseStrategy {
    fn strategy_name(&self) -> &'static str {
        "ArbCloseStrategy"
    }

    fn open_state(&self) -> &OpenStrategyState {
        &self.open_state
    }

    fn open_state_mut(&mut self) -> &mut OpenStrategyState {
        &mut self.open_state
    }

    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    ) {
        warn!(
            "ArbCloseStrategy: strategy_id={} order query {} failed, handoff to arb orphan: client_order_id={}",
            self.open_state.strategy_id, marker, client_order_id
        );
        self.handoff_open_order_to_orphan(client_order_id, marker);
    }

    fn orphan_strategy_role(&self) -> OrphanStrategyRole {
        OrphanStrategyRole::Arb
    }

    fn open_order_rate_bucket(&self) -> OrderRateBucket {
        OrderRateBucket::ArbOpen
    }

    fn open_order_action_log_name(&self) -> &'static str {
        "arb close"
    }

    fn record_terminal_as_arb_close(&self) -> bool {
        true
    }

    fn update_close_inventory_for_close_fill(&self, client_order_id: i64, filled_base_delta: f64) {
        MonitorChannel::instance()
            .apply_close_inventory_fill_delta(client_order_id, filled_base_delta);
    }

    fn release_close_inventory_unfilled(&self, client_order_id: i64, reason: &str) {
        MonitorChannel::instance().release_close_inventory_unfilled(client_order_id, reason);
    }

    fn resolve_open_qty_multiplier(
        &self,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        MonitorChannel::instance().qty_multiplier_for_venue(venue, symbol)
    }
}

impl Strategy for ArbCloseStrategy {
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

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.open_state.strategy_id
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        ArbCloseStrategy::handle_signal(self, signal);
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = self.apply_order_update_common(update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = self.apply_trade_update_common(trade);
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

impl Drop for ArbCloseStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}

fn close_residual_is_effectively_done(open_pos: f64, requested_base_qty: f64) -> bool {
    requested_base_qty.abs() > ARB_CLOSE_QTY_EPS && open_pos.abs() < requested_base_qty.abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_residual_silences_open_pos_below_request() {
        assert!(close_residual_is_effectively_done(7.984725, 640.0));
        assert!(close_residual_is_effectively_done(80.0, 640.0));
    }

    #[test]
    fn close_residual_keeps_logs_at_or_above_request() {
        assert!(!close_residual_is_effectively_done(640.0, 640.0));
        assert!(!close_residual_is_effectively_done(700.0, 640.0));
        assert!(!close_residual_is_effectively_done(1.0, 0.0));
    }
}
