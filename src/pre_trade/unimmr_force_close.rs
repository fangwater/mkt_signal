//! Pre-trade owned UniMMR emergency close execution for funding arbitrage.

use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::strategy::arb_close_strategy::ArbCloseStrategy;
use crate::strategy::manager::{Strategy, StrategyManager};
use account_common::BinanceAccountMode;
use log::{debug, info, warn};
use mkt_parsers::msg::basic_account_msg::{BasicAccountRiskMsg, BasicAccountScope};
use order_common::{OrderType, Side};
use runtime_common::symbol_util::{min_qty_symbol_key, normalize_symbol_for_internal};
use signal_common::common::TradingLeg;
use signal_common::open_signal::ArbOpenCtx;
use std::cell::RefCell;
use std::collections::HashMap;
use trade_signal::ArbMode;

pub const UNIMMR_FORCE_CLOSE_COOLDOWN_US: i64 = 1_000_000;
const POSITION_EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, Default)]
struct ForceCloseScopeState {
    active: bool,
}

#[derive(Debug, Default)]
struct ForceCloseState {
    enabled: bool,
    scopes: HashMap<BasicAccountScope, ForceCloseScopeState>,
    last_batch_ts_us: i64,
}

impl ForceCloseState {
    fn any_active(&self) -> bool {
        self.enabled && self.scopes.values().any(|scope| scope.active)
    }

    fn apply_ratio(
        &mut self,
        scope: BasicAccountScope,
        margin_ratio: f64,
        trigger: f64,
        recover: f64,
    ) -> Option<(bool, bool)> {
        if !self.enabled
            || !margin_ratio.is_finite()
            || !(trigger.is_finite() && recover.is_finite() && trigger < recover)
        {
            return None;
        }

        let global_was_active = self.any_active();
        let was_active = self.scopes.get(&scope).is_some_and(|state| state.active);
        let active = if margin_ratio < trigger {
            true
        } else if margin_ratio > recover {
            false
        } else {
            was_active
        };
        self.scopes.insert(scope, ForceCloseScopeState { active });
        let global_is_active = self.any_active();
        if !global_was_active && global_is_active {
            self.last_batch_ts_us = 0;
        }
        Some((was_active, active))
    }

    fn take_due_batch(&mut self, now_us: i64) -> bool {
        if !self.any_active() || now_us <= 0 {
            return false;
        }
        if self.last_batch_ts_us > 0
            && now_us.saturating_sub(self.last_batch_ts_us) < UNIMMR_FORCE_CLOSE_COOLDOWN_US
        {
            return false;
        }
        self.last_batch_ts_us = now_us;
        true
    }
}

thread_local! {
    static STATE: RefCell<ForceCloseState> = RefCell::new(ForceCloseState::default());
}

pub struct UnimmrForceClose;

impl UnimmrForceClose {
    pub fn initialize(arb_mode: ArbMode, binance_account_mode: Option<BinanceAccountMode>) {
        let enabled = arb_mode == ArbMode::FundingArb
            && !matches!(binance_account_mode, Some(BinanceAccountMode::Standard));
        STATE.with(|state| {
            *state.borrow_mut() = ForceCloseState {
                enabled,
                scopes: HashMap::new(),
                last_batch_ts_us: 0,
            };
        });
        info!(
            "UniMMR Force Close initialized enabled={} arb_mode={} binance_account_mode={:?} cooldown_us={}",
            enabled,
            arb_mode.as_str(),
            binance_account_mode,
            UNIMMR_FORCE_CLOSE_COOLDOWN_US
        );
    }

    pub fn apply_account_risk(scope: BasicAccountScope, msg: &BasicAccountRiskMsg) {
        if matches!(
            scope,
            BasicAccountScope::Unknown
                | BasicAccountScope::BinanceStdSpot
                | BasicAccountScope::BinanceStdUm
        ) {
            return;
        }

        let params = PreTradeParamsLoader::instance();
        let trigger = params.unimmr_force_close_line();
        let recover = params.unimmr_force_close_recover_line();
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            let Some((was_active, active)) =
                state.apply_ratio(scope, msg.margin_ratio, trigger, recover)
            else {
                return;
            };
            if active != was_active {
                warn!(
                    "UniMMR Force Close scope={} {} -> {} margin_ratio={:.6} trigger={:.3} recover={:.3}",
                    scope.as_str(),
                    if was_active { "active" } else { "normal" },
                    if active { "active" } else { "normal" },
                    msg.margin_ratio,
                    trigger,
                    recover
                );
            }
        });
    }

    pub fn is_active() -> bool {
        STATE.with(|state| state.borrow().any_active())
    }

    /// Called by the pre-trade owner thread. Returns the number of activated
    /// first-leg strategies in this force-close batch.
    pub fn drive(now_us: i64) -> usize {
        let due = STATE.with(|state| state.borrow_mut().take_due_batch(now_us));
        if !due {
            return 0;
        }

        let symbols = MonitorChannel::instance().unimmr_force_close_symbols();
        let params = PreTradeParamsLoader::instance();
        let orders_per_symbol = params.open_orders_per_round();
        let mut activated = 0usize;
        for symbol in &symbols {
            let amount_u = params.arb_amount_u_for_symbol(symbol);
            activated = activated.saturating_add(Self::submit_symbol_batch(
                symbol,
                amount_u,
                orders_per_symbol,
                now_us,
            ));
        }

        info!(
            "UniMMR Force Close batch ts_us={} symbols={:?} orders_per_symbol={} activated={}",
            now_us, symbols, orders_per_symbol, activated
        );
        activated
    }

    fn submit_symbol_batch(
        symbol: &str,
        amount_u: f64,
        orders_per_symbol: u32,
        now_us: i64,
    ) -> usize {
        let symbol = normalize_symbol_for_internal(symbol);
        let monitor = MonitorChannel::instance();
        let open_venue = monitor.open_venue();
        let hedge_venue = monitor.hedge_venue();
        let open_position = monitor.get_position_qty(&symbol, open_venue);
        let side = if open_position > POSITION_EPS {
            Side::Sell
        } else if open_position < -POSITION_EPS {
            Side::Buy
        } else {
            debug!(
                "UniMMR Force Close skip symbol={} because opening-leg position is zero venue={:?}",
                symbol, open_venue
            );
            return 0;
        };
        let mark_price = monitor
            .price_table()
            .borrow()
            .mark_price(&symbol)
            .unwrap_or(0.0);
        if !(mark_price.is_finite() && mark_price > 0.0) {
            warn!(
                "UniMMR Force Close skip symbol={} because mark price is unavailable",
                symbol
            );
            return 0;
        }
        if !(amount_u.is_finite() && amount_u > 0.0) || orders_per_symbol == 0 {
            warn!(
                "UniMMR Force Close skip symbol={} because batch params are invalid amount_u={} orders_per_symbol={}",
                symbol, amount_u, orders_per_symbol
            );
            return 0;
        }

        let qty_multiplier = match monitor.qty_multiplier_for_venue(open_venue, &symbol) {
            Ok(multiplier) if multiplier.is_finite() && multiplier > 0.0 => multiplier,
            Ok(multiplier) => {
                warn!(
                    "UniMMR Force Close skip symbol={} invalid qty multiplier={} venue={:?}",
                    symbol, multiplier, open_venue
                );
                return 0;
            }
            Err(err) => {
                warn!(
                    "UniMMR Force Close skip symbol={} resolve qty multiplier failed venue={:?}: {}",
                    symbol, open_venue, err
                );
                return 0;
            }
        };
        let venue_qty = amount_u / mark_price / qty_multiplier;
        let Some(table) = monitor.try_venue_min_qty_table(open_venue) else {
            warn!(
                "UniMMR Force Close skip symbol={} because min-qty table is unavailable venue={:?}",
                symbol, open_venue
            );
            return 0;
        };
        let symbol_key = min_qty_symbol_key(open_venue, &symbol);
        let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);

        let strategy_mgr = monitor.strategy_mgr();
        strategy_mgr
            .borrow_mut()
            .ensure_arb_hedge_strategy_for_normalized_symbol(&symbol);

        let mut activated = 0usize;
        for child_index in 0..orders_per_symbol {
            let mut ctx = ArbOpenCtx::new();
            ctx.opening_leg = TradingLeg::new(open_venue, mark_price, mark_price, now_us);
            ctx.hedging_leg = TradingLeg::new(hedge_venue, mark_price, mark_price, now_us);
            ctx.set_opening_symbol(&symbol);
            ctx.set_hedging_symbol(&symbol);
            ctx.set_side(side);
            ctx.set_order_type(OrderType::Market);
            ctx.set_price_with_tick_floor(mark_price, price_tick);
            ctx.set_amount_with_tick_floor(venue_qty, qty_tick);
            ctx.exp_time = 0;
            ctx.create_ts = now_us;
            ctx.hedge_timeout_us = 0;
            ctx.set_from_key(format!("unimmr_force_close|{now_us}|{child_index}").into_bytes());
            if ctx.price_count() <= 0 || ctx.amount_count() <= 0 {
                warn!(
                    "UniMMR Force Close skip quantized child symbol={} venue_qty={:.12} qty_tick={:.12} mark_price={:.8} price_tick={:.12}",
                    symbol, venue_qty, qty_tick, mark_price, price_tick
                );
                continue;
            }

            let strategy_id = StrategyManager::generate_strategy_id();
            let mut strategy = ArbCloseStrategy::new_force(strategy_id);
            strategy.handle_arb_close_ctx_with_symbols(ctx, symbol.clone(), symbol.clone());
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(Box::new(strategy));
                activated = activated.saturating_add(1);
            }
        }
        activated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_close_hysteresis_is_independent() {
        let mut state = ForceCloseState {
            enabled: true,
            ..ForceCloseState::default()
        };
        let scope = BasicAccountScope::BinanceUnified;

        assert_eq!(
            state.apply_ratio(scope, 1.4, 1.3, 1.5),
            Some((false, false))
        );
        assert!(!state.any_active());
        assert_eq!(state.apply_ratio(scope, 1.2, 1.3, 1.5), Some((false, true)));
        assert!(state.any_active());
        assert_eq!(state.apply_ratio(scope, 1.4, 1.3, 1.5), Some((true, true)));
        assert!(state.any_active());
        assert_eq!(state.apply_ratio(scope, 1.6, 1.3, 1.5), Some((true, false)));
        assert!(!state.any_active());
    }

    #[test]
    fn force_close_batch_uses_its_own_one_second_cooldown() {
        let mut state = ForceCloseState {
            enabled: true,
            ..ForceCloseState::default()
        };
        state.apply_ratio(BasicAccountScope::BinanceUnified, 1.2, 1.3, 1.5);

        assert!(state.take_due_batch(1));
        assert!(!state.take_due_batch(UNIMMR_FORCE_CLOSE_COOLDOWN_US));
        assert!(state.take_due_batch(UNIMMR_FORCE_CLOSE_COOLDOWN_US + 1));
    }
}
