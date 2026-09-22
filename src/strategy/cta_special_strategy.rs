use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::strategy::arb_close_strategy::ArbCloseStrategy;
use crate::strategy::manager::{OrderTerminalRecorder, Strategy, StrategyManager};
use log::{debug, info, warn};
use order_common::{OrderType, OrderUpdate, Side, TradeEngineResponse, TradeUpdate, TradingVenue};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use signal_common::common::TradingLeg;
use signal_common::open_signal::ArbOpenCtx;
use signal_common::trade_signal::TradeSignal;
use std::any::Any;

const QTY_EPS: f64 = 1e-12;
const EXIT_RETRY_US: i64 = 5_000_000;
const POSITION_SYNC_GRACE_US: i64 = 2_000_000;
const MIN_EXIT_DELAY_US: i64 = 1_000_000;
const MAX_BBO_AGE_US: i64 = 5_000_000;

#[derive(Debug, Clone, PartialEq)]
pub struct CtaSpecialExitConfig {
    pub rule_name: String,
    pub factor_exit_enabled: bool,
    pub factor_exit_quantile_long: f64,
    pub factor_exit_quantile_short: f64,
    pub trailing_stop_enabled: bool,
    pub trailing_stop_trigger_step: f64,
    pub trailing_stop_move_step: f64,
    pub max_holding_seconds: i64,
}

impl CtaSpecialExitConfig {
    pub fn from_open_from_key(from_key: &[u8]) -> Option<Self> {
        let raw = std::str::from_utf8(from_key).ok()?;
        let value = |key: &str| raw.split(':').find_map(|field| field.strip_prefix(key));
        if value("cta_special=")? != "1" {
            return None;
        }
        let config = Self {
            rule_name: value("cta_rule=")?.to_string(),
            factor_exit_enabled: value("cta_factor_exit=")
                .map(|raw| matches!(raw, "1" | "true"))
                .unwrap_or(true),
            factor_exit_quantile_long: value("cta_exit_long=")?.parse().ok()?,
            factor_exit_quantile_short: value("cta_exit_short=")?.parse().ok()?,
            trailing_stop_enabled: matches!(value("cta_trailing=")?, "1" | "true"),
            trailing_stop_trigger_step: value("cta_trigger=")?.parse().ok()?,
            trailing_stop_move_step: value("cta_move=")?.parse().ok()?,
            max_holding_seconds: value("cta_max_hold_s=")
                .and_then(|raw| raw.parse().ok())
                .unwrap_or(0),
        };
        config.validate().then_some(config)
    }

    fn validate(&self) -> bool {
        !self.rule_name.is_empty()
            && self.factor_exit_quantile_long.is_finite()
            && (0.0..=1.0).contains(&self.factor_exit_quantile_long)
            && self.factor_exit_quantile_short.is_finite()
            && (0.0..=1.0).contains(&self.factor_exit_quantile_short)
            && self.max_holding_seconds >= 0
            && (!self.trailing_stop_enabled
                || (self.trailing_stop_trigger_step.is_finite()
                    && self.trailing_stop_trigger_step > 0.0
                    && self.trailing_stop_move_step.is_finite()
                    && self.trailing_stop_move_step > 0.0
                    && self.trailing_stop_move_step < self.trailing_stop_trigger_step))
    }
}

#[derive(Debug, Clone)]
struct CtaSpecialLot {
    side: Side,
    qty: f64,
    entry_price: f64,
    entry_ts: i64,
    earliest_exit_ts: i64,
    trailing_level: u32,
    exit_requested_ts: i64,
    config: CtaSpecialExitConfig,
}

impl CtaSpecialLot {
    fn signed_qty(&self) -> f64 {
        match self.side {
            Side::Buy => self.qty,
            Side::Sell => -self.qty,
        }
    }

    fn factor_exit(&self, quantile: Option<f64>) -> bool {
        if !self.config.factor_exit_enabled {
            return false;
        }
        let Some(quantile) = quantile.filter(|value| value.is_finite()) else {
            return false;
        };
        match self.side {
            Side::Buy => quantile < self.config.factor_exit_quantile_long,
            Side::Sell => quantile > self.config.factor_exit_quantile_short,
        }
    }

    fn max_holding_exit(&self, now_ts: i64) -> bool {
        self.config.max_holding_seconds > 0
            && now_ts.saturating_sub(self.entry_ts)
                >= self.config.max_holding_seconds.saturating_mul(1_000_000)
    }

    fn trailing_exit(&mut self, executable_price: f64) -> bool {
        if !self.config.trailing_stop_enabled
            || !executable_price.is_finite()
            || executable_price <= 0.0
            || !self.entry_price.is_finite()
            || self.entry_price <= 0.0
        {
            return false;
        }
        let direction = if self.side == Side::Buy { 1.0 } else { -1.0 };
        let favorable_return = direction * (executable_price / self.entry_price - 1.0);
        if favorable_return >= self.config.trailing_stop_trigger_step {
            let level = (favorable_return / self.config.trailing_stop_trigger_step).floor() as u32;
            self.trailing_level = self.trailing_level.max(level.max(1));
        }
        if self.trailing_level == 0 {
            return false;
        }
        let stop = self.entry_price
            * (1.0 + direction * self.trailing_level as f64 * self.config.trailing_stop_move_step);
        match self.side {
            Side::Buy => executable_price <= stop,
            Side::Sell => executable_price >= stop,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CtaSpecialSnapshot {
    pub symbol: String,
    pub venue: TradingVenue,
    pub tracked_signed_qty: f64,
    pub lot_count: usize,
    pub latest_quantile: Option<f64>,
    pub latest_model_ts_ms: i64,
}

pub struct CtaSpecialStrategy {
    strategy_id: i32,
    symbol: String,
    venue: TradingVenue,
    lots: Vec<CtaSpecialLot>,
    latest_config: Option<CtaSpecialExitConfig>,
    latest_quantile: Option<f64>,
    latest_model_ts_ms: i64,
    last_open_record_ts: i64,
}

impl CtaSpecialStrategy {
    pub fn new(strategy_id: i32, symbol: String, venue: TradingVenue) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol),
            venue,
            lots: Vec::new(),
            latest_config: None,
            latest_quantile: None,
            latest_model_ts_ms: 0,
            last_open_record_ts: 0,
        }
    }

    pub fn apply_factor_update(
        &mut self,
        model_ts_ms: i64,
        score_quantile: Option<f64>,
        score_ready: bool,
        config: CtaSpecialExitConfig,
    ) -> bool {
        if model_ts_ms <= self.latest_model_ts_ms || !config.validate() {
            return false;
        }
        self.latest_model_ts_ms = model_ts_ms;
        self.latest_quantile = score_quantile.filter(|quantile| {
            score_ready && quantile.is_finite() && (0.0..=1.0).contains(quantile)
        });
        self.latest_config = Some(config);
        true
    }

    pub fn has_opposite_position(&self, opening_side: Side) -> bool {
        let tracked = self.tracked_signed_qty();
        match opening_side {
            Side::Buy => tracked < -QTY_EPS,
            Side::Sell => tracked > QTY_EPS,
        }
    }

    pub fn snapshot(&self) -> CtaSpecialSnapshot {
        CtaSpecialSnapshot {
            symbol: self.symbol.clone(),
            venue: self.venue,
            tracked_signed_qty: self.tracked_signed_qty(),
            lot_count: self.lots.len(),
            latest_quantile: self.latest_quantile,
            latest_model_ts_ms: self.latest_model_ts_ms,
        }
    }

    fn tracked_signed_qty(&self) -> f64 {
        self.lots.iter().map(CtaSpecialLot::signed_qty).sum()
    }

    fn sync_to_account_position(&mut self, now_ts: i64, actual: f64, recovery_mark_price: f64) {
        if !actual.is_finite() {
            return;
        }
        let tracked = self.tracked_signed_qty();
        if actual.abs() <= QTY_EPS {
            if now_ts.saturating_sub(self.last_open_record_ts) >= POSITION_SYNC_GRACE_US {
                self.lots.clear();
            }
            return;
        }
        let same_direction = tracked.abs() <= QTY_EPS || tracked.signum() == actual.signum();
        if !same_direction {
            self.lots.clear();
        }
        if now_ts.saturating_sub(self.last_open_record_ts) < POSITION_SYNC_GRACE_US {
            return;
        }

        let tracked_abs = self.tracked_signed_qty().abs();
        let actual_abs = actual.abs();
        if tracked_abs > actual_abs + QTY_EPS {
            let mut excess = tracked_abs - actual_abs;
            for lot in &mut self.lots {
                let reduced = excess.min(lot.qty);
                lot.qty -= reduced;
                excess -= reduced;
                if excess <= QTY_EPS {
                    break;
                }
            }
            self.lots.retain(|lot| lot.qty > QTY_EPS);
        } else if actual_abs > tracked_abs + QTY_EPS {
            let Some(config) = self.latest_config.clone() else {
                return;
            };
            if !(recovery_mark_price.is_finite() && recovery_mark_price > 0.0) {
                return;
            }
            let side = if actual > 0.0 { Side::Buy } else { Side::Sell };
            let qty = actual_abs - tracked_abs;
            self.lots.push(CtaSpecialLot {
                side,
                qty,
                entry_price: recovery_mark_price,
                entry_ts: now_ts,
                earliest_exit_ts: now_ts.saturating_add(MIN_EXIT_DELAY_US),
                trailing_level: 0,
                exit_requested_ts: 0,
                config,
            });
            warn!(
                "CtaSpecial recovered untracked position as synthetic lot symbol={} side={} qty={:.8} entry_mark={:.8} total_position={:.8} trailing_enabled={}",
                self.symbol,
                side.as_str(),
                qty,
                recovery_mark_price,
                actual,
                self.lots
                    .last()
                    .is_some_and(|lot| lot.config.trailing_stop_enabled)
            );
        }
    }

    fn exit_plan(&mut self, now_ts: i64, bid: f64, ask: f64) -> Option<(Side, f64, &'static str)> {
        let mut close_side = None;
        let mut qty = 0.0;
        let mut reason = "max_holding";
        let mut reason_priority = 0;
        for lot in &mut self.lots {
            if now_ts < lot.earliest_exit_ts
                || (lot.exit_requested_ts > 0
                    && now_ts.saturating_sub(lot.exit_requested_ts) < EXIT_RETRY_US)
            {
                continue;
            }
            let matching_factor_quantile = self
                .latest_config
                .as_ref()
                .filter(|latest| latest.rule_name == lot.config.rule_name)
                .and(self.latest_quantile);
            let factor_exit = lot.factor_exit(matching_factor_quantile);
            let executable_price = if lot.side == Side::Buy { bid } else { ask };
            let trailing_exit = lot.trailing_exit(executable_price);
            let max_holding_exit = lot.max_holding_exit(now_ts);
            if !factor_exit && !trailing_exit && !max_holding_exit {
                continue;
            }
            let lot_close_side = if lot.side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            if close_side.is_some_and(|side| side != lot_close_side) {
                continue;
            }
            close_side = Some(lot_close_side);
            qty += lot.qty;
            lot.exit_requested_ts = now_ts;
            if trailing_exit && reason_priority < 3 {
                reason = "trailing_stop";
                reason_priority = 3;
            } else if factor_exit && reason_priority < 2 {
                reason = "factor_exit";
                reason_priority = 2;
            } else if max_holding_exit && reason_priority < 1 {
                reason = "max_holding";
                reason_priority = 1;
            }
        }
        close_side.map(|side| (side, qty, reason))
    }

    fn cancel_decayed_opening_makers(&self, now_ts: i64) {
        let Some(quantile) = self.latest_quantile else {
            return;
        };
        let Some(config) = self.latest_config.as_ref() else {
            return;
        };
        if !config.factor_exit_enabled {
            return;
        }
        let manager = MonitorChannel::instance().strategy_mgr();
        let mut manager = manager.borrow_mut();
        if quantile < config.factor_exit_quantile_long {
            manager.cancel_cta_special_opening_makers(
                &self.symbol,
                Side::Buy,
                now_ts,
                "cta_special_factor_decay_long",
            );
        }
        if quantile > config.factor_exit_quantile_short {
            manager.cancel_cta_special_opening_makers(
                &self.symbol,
                Side::Sell,
                now_ts,
                "cta_special_factor_decay_short",
            );
        }
    }

    fn submit_close(&self, now_ts: i64, close_side: Side, requested_qty: f64, reason: &str) {
        let position = MonitorChannel::instance().get_position_qty(&self.symbol, self.venue);
        let position_matches = match close_side {
            Side::Sell => position > QTY_EPS,
            Side::Buy => position < -QTY_EPS,
        };
        if !position_matches {
            return;
        }
        let quote = match trade_signal::MktChannel::instance().get_quote(&self.symbol, self.venue) {
            Some(quote) => quote,
            None => return,
        };
        let price = if close_side == Side::Sell {
            quote.bid
        } else {
            quote.ask
        };
        let qty = requested_qty.min(position.abs());
        let Some(table) = MonitorChannel::instance().venue_min_qty_table(self.venue) else {
            return;
        };
        let Some(price_tick) = table.price_tick(&self.symbol) else {
            return;
        };
        let Some(qty_tick) = table.step_size(&self.symbol) else {
            return;
        };
        let mut ctx = ArbOpenCtx::new();
        ctx.opening_leg = TradingLeg::new_with_qty(
            self.venue,
            quote.bid,
            quote.bid_qty,
            quote.ask,
            quote.ask_qty,
            quote.ts,
        );
        ctx.hedging_leg = ctx.opening_leg;
        ctx.set_opening_symbol(&self.symbol);
        ctx.set_hedging_symbol(&self.symbol);
        ctx.set_side(close_side);
        ctx.set_order_type(OrderType::Market);
        if !ctx.set_price_with_tick_floor(price, price_tick)
            || !ctx.set_amount_with_tick_floor(qty, qty_tick)
            || ctx.amount_value() <= QTY_EPS
        {
            return;
        }
        ctx.create_ts = now_ts;
        ctx.exp_time = 0;
        ctx.set_from_key(
            format!(
                "cta_special_exit=1:cta_rule={}:reason={}",
                self.latest_config
                    .as_ref()
                    .map(|config| config.rule_name.as_str())
                    .unwrap_or("recovered"),
                reason
            )
            .into_bytes(),
        );

        let strategy_id = StrategyManager::generate_strategy_id();
        let mut strategy = ArbCloseStrategy::new(strategy_id);
        strategy.handle_arb_close_ctx(ctx);
        if strategy.is_active() {
            info!(
                "CtaSpecial submit reduce-only market exit symbol={} side={} qty={:.8} position={:.8} reason={}",
                self.symbol,
                close_side.as_str(),
                qty,
                position,
                reason
            );
            MonitorChannel::instance()
                .strategy_mgr()
                .borrow_mut()
                .insert(Box::new(strategy));
        }
    }
}

impl Strategy for CtaSpecialStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn is_strategy_order(&self, _order_id: i64) -> bool {
        false
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

    fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

    fn handle_period_clock(&mut self, current_tp: i64) {
        let now_ts = if current_tp > 0 {
            current_tp
        } else {
            get_timestamp_us()
        };
        let position = MonitorChannel::instance().get_position_qty(&self.symbol, self.venue);
        let recovery_mark_price = MonitorChannel::instance()
            .mark_price_for_symbol(&self.symbol)
            .unwrap_or(0.0);
        self.sync_to_account_position(now_ts, position, recovery_mark_price);

        let Some(quote) = trade_signal::MktChannel::instance().get_quote(&self.symbol, self.venue)
        else {
            return;
        };
        if quote.ts <= 0 || now_ts.saturating_sub(quote.ts) > MAX_BBO_AGE_US {
            return;
        }
        self.cancel_decayed_opening_makers(now_ts);
        if let Some((side, qty, reason)) = self.exit_plan(now_ts, quote.bid, quote.ask) {
            self.submit_close(now_ts, side, qty, reason);
        }
    }

    fn is_active(&self) -> bool {
        true
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }

    fn has_order_terminal_recorder(&self) -> bool {
        true
    }

    fn order_terminal_recorder_mut(&mut self) -> Option<&mut dyn OrderTerminalRecorder> {
        Some(self)
    }
}

impl OrderTerminalRecorder for CtaSpecialStrategy {
    fn record_open_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        _order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        _close_ts: i64,
        open_client_order_id: i64,
        open_from_key: &[u8],
    ) -> bool {
        let Some(config) = CtaSpecialExitConfig::from_open_from_key(open_from_key) else {
            return false;
        };
        let qty = filled_base_qty.abs();
        if qty <= QTY_EPS || !price.is_finite() || price <= 0.0 {
            return false;
        }
        if self.latest_config.is_none() {
            self.latest_config = Some(config.clone());
        }
        self.last_open_record_ts = terminal_ts;
        self.lots.push(CtaSpecialLot {
            side,
            qty,
            entry_price: price,
            entry_ts: terminal_ts,
            earliest_exit_ts: terminal_ts.saturating_add(MIN_EXIT_DELAY_US),
            trailing_level: 0,
            exit_requested_ts: 0,
            config,
        });
        debug!(
            "CtaSpecial recorded open lot symbol={} order_id={} side={} qty={:.8} entry={:.8}",
            self.symbol,
            open_client_order_id,
            side.as_str(),
            qty,
            price
        );
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        _terminal_ts: i64,
        _side: Side,
        _order_base_qty: f64,
        _filled_base_qty: f64,
        _price: f64,
        _bound_open_client_order_id: i64,
        _hedge_client_order_id: i64,
    ) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CtaSpecialExitConfig {
        CtaSpecialExitConfig {
            rule_name: "test".to_string(),
            factor_exit_enabled: true,
            factor_exit_quantile_long: 0.3,
            factor_exit_quantile_short: 0.7,
            trailing_stop_enabled: true,
            trailing_stop_trigger_step: 0.01,
            trailing_stop_move_step: 0.005,
            max_holding_seconds: 0,
        }
    }

    #[test]
    fn parses_special_open_contract() {
        let parsed = CtaSpecialExitConfig::from_open_from_key(
            b"cta_special=1:cta_rule=tp_vpi_018:cta_exit_long=0.3:cta_exit_short=0.7:cta_trailing=1:cta_trigger=0.02:cta_move=0.01",
        )
        .expect("config");
        assert_eq!(parsed.rule_name, "tp_vpi_018");
        assert_eq!(parsed.trailing_stop_trigger_step, 0.02);
    }

    #[test]
    fn trailing_only_arms_after_favorable_trigger() {
        let mut lot = CtaSpecialLot {
            side: Side::Buy,
            qty: 1.0,
            entry_price: 100.0,
            entry_ts: 0,
            earliest_exit_ts: 0,
            trailing_level: 0,
            exit_requested_ts: 0,
            config: config(),
        };
        assert!(!lot.trailing_exit(99.0));
        assert!(!lot.trailing_exit(101.1));
        assert_eq!(lot.trailing_level, 1);
        assert!(!lot.trailing_exit(100.6));
        assert!(lot.trailing_exit(100.4));
    }

    #[test]
    fn short_factor_exit_uses_upper_threshold() {
        let lot = CtaSpecialLot {
            side: Side::Sell,
            qty: 1.0,
            entry_price: 100.0,
            entry_ts: 0,
            earliest_exit_ts: 0,
            trailing_level: 0,
            exit_requested_ts: 0,
            config: config(),
        };
        assert!(!lot.factor_exit(Some(0.7)));
        assert!(lot.factor_exit(Some(0.71)));
    }

    #[test]
    fn disabled_factor_exit_does_not_close_or_cancel_by_quantile() {
        let mut disabled = config();
        disabled.factor_exit_enabled = false;
        let lot = CtaSpecialLot {
            side: Side::Buy,
            qty: 1.0,
            entry_price: 100.0,
            entry_ts: 0,
            earliest_exit_ts: 0,
            trailing_level: 0,
            exit_requested_ts: 0,
            config: disabled,
        };
        assert!(!lot.factor_exit(Some(0.0)));
    }

    #[test]
    fn max_holding_uses_lot_entry_timestamp() {
        let mut max_hold = config();
        max_hold.factor_exit_enabled = false;
        max_hold.trailing_stop_enabled = false;
        max_hold.max_holding_seconds = 10;
        let lot = CtaSpecialLot {
            side: Side::Buy,
            qty: 1.0,
            entry_price: 100.0,
            entry_ts: 5_000_000,
            earliest_exit_ts: 0,
            trailing_level: 0,
            exit_requested_ts: 0,
            config: max_hold,
        };
        assert!(!lot.max_holding_exit(14_999_999));
        assert!(lot.max_holding_exit(15_000_000));
    }

    #[test]
    fn position_sync_reduces_exact_excess_across_lots() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);
        for entry_price in [100.0, 101.0] {
            strategy.lots.push(CtaSpecialLot {
                side: Side::Buy,
                qty: 1.0,
                entry_price,
                entry_ts: 0,
                earliest_exit_ts: 0,
                trailing_level: 0,
                exit_requested_ts: 0,
                config: config(),
            });
        }

        strategy.sync_to_account_position(POSITION_SYNC_GRACE_US, 1.4, 102.0);

        assert!((strategy.tracked_signed_qty() - 1.4).abs() < QTY_EPS);
        assert_eq!(strategy.lots.len(), 2);
        assert!((strategy.lots[0].qty - 0.4).abs() < QTY_EPS);
        assert!((strategy.lots[1].qty - 1.0).abs() < QTY_EPS);
    }

    #[test]
    fn recovered_position_uses_mark_price_and_keeps_trailing() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);
        strategy.latest_config = Some(config());

        strategy.sync_to_account_position(POSITION_SYNC_GRACE_US, 1.0, 100.0);

        assert_eq!(strategy.lots.len(), 1);
        assert_eq!(strategy.lots[0].side, Side::Buy);
        assert!((strategy.lots[0].qty - 1.0).abs() < QTY_EPS);
        assert!((strategy.lots[0].entry_price - 100.0).abs() < QTY_EPS);
        assert!(strategy.lots[0].config.trailing_stop_enabled);
        assert!(strategy.lots[0].factor_exit(Some(0.29)));
        assert!(!strategy.lots[0].trailing_exit(101.1));
        assert!(strategy.lots[0].trailing_exit(100.4));
    }

    #[test]
    fn direction_mismatch_recovers_total_position_as_one_mark_price_lot() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);
        strategy.latest_config = Some(config());
        strategy.lots.push(CtaSpecialLot {
            side: Side::Buy,
            qty: 0.5,
            entry_price: 101.0,
            entry_ts: 0,
            earliest_exit_ts: 0,
            trailing_level: 0,
            exit_requested_ts: 0,
            config: config(),
        });

        strategy.sync_to_account_position(POSITION_SYNC_GRACE_US, -2.0, 98.0);

        assert_eq!(strategy.lots.len(), 1);
        assert_eq!(strategy.lots[0].side, Side::Sell);
        assert!((strategy.lots[0].qty - 2.0).abs() < QTY_EPS);
        assert!((strategy.lots[0].entry_price - 98.0).abs() < QTY_EPS);
        assert!((strategy.tracked_signed_qty() + 2.0).abs() < QTY_EPS);
    }

    #[test]
    fn recovered_position_waits_for_a_valid_mark_price() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);
        strategy.latest_config = Some(config());

        strategy.sync_to_account_position(POSITION_SYNC_GRACE_US, 1.0, 0.0);

        assert!(strategy.lots.is_empty());
    }

    #[test]
    fn direct_factor_application_still_validates_config() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);
        let mut invalid = config();
        invalid.factor_exit_quantile_long = f64::NAN;

        assert!(!strategy.apply_factor_update(1, Some(0.5), true, invalid));
        assert_eq!(strategy.latest_model_ts_ms, 0);
        assert!(strategy.latest_config.is_none());
    }

    #[test]
    fn direct_model_updates_are_monotonic_and_clear_unready_quantiles() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);

        assert!(strategy.apply_factor_update(1, Some(0.2), true, config()));
        assert_eq!(strategy.latest_quantile, Some(0.2));
        assert!(!strategy.apply_factor_update(1, Some(0.1), true, config()));
        assert_eq!(strategy.latest_quantile, Some(0.2));

        assert!(strategy.apply_factor_update(2, Some(0.1), false, config()));
        assert_eq!(strategy.latest_quantile, None);
        assert_eq!(strategy.latest_model_ts_ms, 2);
    }

    #[test]
    fn factor_exit_requires_matching_lot_rule() {
        let mut strategy =
            CtaSpecialStrategy::new(1, "BTCUSDT".to_string(), TradingVenue::BinanceFutures);
        strategy.latest_quantile = Some(0.2);
        let mut latest = config();
        latest.rule_name = "baseline_104".to_string();
        strategy.latest_config = Some(latest);
        strategy.lots.push(CtaSpecialLot {
            side: Side::Buy,
            qty: 1.0,
            entry_price: 100.0,
            entry_ts: 0,
            earliest_exit_ts: 0,
            trailing_level: 0,
            exit_requested_ts: 0,
            config: config(),
        });

        assert!(strategy.exit_plan(1, 100.0, 100.1).is_none());
    }
}
