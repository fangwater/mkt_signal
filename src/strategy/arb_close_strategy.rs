use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::OrderRateBucket;
use crate::pre_trade::order_manager::Side;
use crate::pre_trade::PersistChannel;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::{OrphanStrategyRole, Strategy};
use crate::strategy::open_strategy_common::{
    OpenSignalInput, OpenStrategyCommon, OpenStrategyState,
};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, info, warn};
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
            info!(
                "ArbCloseStrategy: strategy_id={} skip because pending limit order limit hit symbol={} venue={:?} side={:?} open_pos={:.8} signal_qty={:.8} reason={}",
                self.open_state.strategy_id,
                symbol,
                venue,
                side,
                open_pos,
                ctx.amount_value(),
                e
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
        let grant = MonitorChannel::instance().reserve_close_inventory(
            venue,
            &symbol,
            side,
            requested_base_qty,
            client_order_id,
        );
        if grant.granted_base_qty <= ARB_CLOSE_QTY_EPS {
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
            self.open_state.alive = false;
            return;
        }
        let order_qty = grant.granted_base_qty / qty_multiplier;
        if order_qty + ARB_CLOSE_QTY_EPS < ctx.amount_value() {
            info!(
                "ArbCloseStrategy: strategy_id={} clip close qty by close inventory symbol={} venue={:?} side={:?} signal_qty={:.8} order_qty={:.8} requested_base={:.8} granted_base={:.8} inventory={:.8}",
                self.open_state.strategy_id,
                symbol,
                venue,
                side,
                ctx.amount_value(),
                order_qty,
                requested_base_qty,
                grant.granted_base_qty,
                grant.closable_inventory_base
            );
        }

        // 方向 & net 都通过后，再做 min_qty / min_notional 检查。
        // close 不像 open 那样"凑齐到 min"——残余仓位本来就少，凑齐会过头去开反向仓位。
        // 因此这里只查不补：低于最小要求 → info! 打印具体原因并跳过整张单。
        let price_hint = {
            let p = ctx.price_value();
            if p > 0.0 {
                Some(p)
            } else {
                None
            }
        };
        if let Err(reason) = MonitorChannel::instance()
            .check_min_trading_requirements(venue, &symbol, order_qty, price_hint)
        {
            MonitorChannel::instance()
                .release_close_inventory_unfilled(client_order_id, "below_min_trade_requirement");
            info!(
                "ArbCloseStrategy: strategy_id={} skip below min trade requirements symbol={} venue={:?} open_pos={:.8} signal_qty={:.8} price_hint={:?} reason={}",
                self.open_state.strategy_id,
                symbol,
                venue,
                open_pos,
                order_qty,
                price_hint,
                reason
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
            price: ctx.price_value(),
            price_count: ctx.price_count(),
            amount_count: ctx.amount_count(),
            exp_time: ctx.exp_time,
            create_ts: ctx.create_ts,
            from_key_len: ctx.from_key_len,
            from_key: ctx.from_key,
            price_qv: ctx.price_qv,
            price_offset: ctx.price_offset,
            reduce_only: true,
            client_order_id: Some(client_order_id),
            close_ts: 0,
            mkt_ts,
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

    fn skip_open_position_risk_checks(&self) -> bool {
        true
    }

    fn enable_open_order_rate_limit(&self) -> bool {
        false
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
