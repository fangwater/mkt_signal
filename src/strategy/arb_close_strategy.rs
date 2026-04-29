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

fn base_to_venue_qty(base_qty: f64, multiplier: f64, leg_name: &str) -> Result<f64, String> {
    if multiplier <= 0.0 {
        return Err(format!("{} multiplier invalid: {}", leg_name, multiplier));
    }
    Ok(base_qty / multiplier)
}

/// Arb 强平开仓腿：复用 common open 下单生命周期，只把 open leg reduce-only 平到 0。
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
        let close_base_qty = match side {
            Side::Sell if open_pos > ARB_CLOSE_QTY_EPS => open_pos,
            Side::Buy if open_pos < -ARB_CLOSE_QTY_EPS => -open_pos,
            _ => {
                info!(
                    "ArbCloseStrategy: strategy_id={} skip because close side does not reduce open position symbol={} venue={:?} side={:?} open_pos={:.8}",
                    self.open_state.strategy_id, symbol, venue, side, open_pos
                );
                self.open_state.alive = false;
                return;
            }
        };

        let qty_multiplier = match MonitorChannel::instance()
            .qty_multiplier_for_venue(venue, &symbol)
        {
            Ok(multiplier) => multiplier,
            Err(err) => {
                warn!(
                    "ArbCloseStrategy: strategy_id={} qty multiplier unavailable symbol={} venue={:?}: {}",
                    self.open_state.strategy_id, symbol, venue, err
                );
                self.open_state.alive = false;
                return;
            }
        };
        let close_venue_qty = match base_to_venue_qty(close_base_qty, qty_multiplier, "arb_close") {
            Ok(qty) => qty,
            Err(err) => {
                warn!(
                    "ArbCloseStrategy: strategy_id={} close qty convert failed symbol={} venue={:?} base_qty={:.8}: {}",
                    self.open_state.strategy_id, symbol, venue, close_base_qty, err
                );
                self.open_state.alive = false;
                return;
            }
        };
        ctx.set_amount_from_value_floor(close_venue_qty);
        if ctx.amount_value() <= ARB_CLOSE_QTY_EPS || ctx.amount_count() <= 0 {
            info!(
                "ArbCloseStrategy: strategy_id={} skip because aligned close qty is zero symbol={} venue={:?} base_qty={:.8} raw_venue_qty={:.8}",
                self.open_state.strategy_id, symbol, venue, close_base_qty, close_venue_qty
            );
            self.open_state.alive = false;
            return;
        }

        let _ = self.handle_open_signal_common(OpenSignalInput {
            signal_kind: "ArbClose",
            order_log_name: "ArbClose",
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
            reduce_only: true,
            close_ts: 0,
        });
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

    // close 单是镜像 ArbHedge 的 reduce-only 单，不交给 orphan：orphan 会把 close 单的
    // terminal 当普通 open 释放 pending，导致 ArbHedge 重复对冲。这里 query 发不出去时
    // 只 warn，不清 watchdog —— 下一轮 period clock 会重试，交易所终态最终也会通过
    // apply_order_update_common 闭环。极端的 publisher + 交易所同时长时间故障需要运维处理。
    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    ) {
        warn!(
            "ArbCloseStrategy: strategy_id={} order query {} failed, keep force-close order local: client_order_id={}",
            self.open_state.strategy_id, marker, client_order_id
        );
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
