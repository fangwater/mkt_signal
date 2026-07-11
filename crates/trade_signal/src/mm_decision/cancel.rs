use anyhow::Result;
use log::{debug, warn};
use std::collections::HashMap;

use super::super::mkt_channel::MktChannel;
use super::super::symbol_list::SymbolList;
use super::from_key::build_mm_cancel_from_key;
use super::state::MmDecisionState;
use crate::model_output_hub::ModelOutputUpdateEvent;
use crate::return_score_threshold::ReturnScoreCancelThresholds;
use mkt_parsers::symbol_match::normalize_symbol_for_whitelist;
use order_common::Side;
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use signal_common::trade_signal::SignalType;

pub(crate) struct MmCancelDecision {
    last_cancel_ts_us: HashMap<String, i64>,
}

fn return_score_cancel_hits(score: f64, thresholds: ReturnScoreCancelThresholds) -> (bool, bool) {
    let sell_cancel_hit = thresholds
        .sell_cancel_score_threshold
        .is_some_and(|threshold| score > threshold);
    let buy_cancel_hit = thresholds
        .buy_cancel_score_threshold
        .is_some_and(|threshold| score < threshold);
    (sell_cancel_hit, buy_cancel_hit)
}

impl MmCancelDecision {
    pub(crate) fn new() -> Self {
        Self {
            last_cancel_ts_us: HashMap::new(),
        }
    }

    fn cancel_throttle_key(symbol: &str, side: Side) -> String {
        format!(
            "{}:{}",
            normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures),
            side.as_str()
        )
    }

    fn should_emit(last_ts_us: Option<i64>, now_us: i64, tlen_cancel_freq_ms: u64) -> bool {
        let interval_us = (tlen_cancel_freq_ms as i64).saturating_mul(1_000);
        let last_ts_us = last_ts_us.unwrap_or_default();
        last_ts_us == 0 || now_us.saturating_sub(last_ts_us) >= interval_us
    }

    fn cancel_throttled(
        &self,
        symbol: &str,
        side: Side,
        state: &MmDecisionState,
        now_us: i64,
    ) -> bool {
        let key = Self::cancel_throttle_key(symbol, side);
        let should_emit = Self::should_emit(
            self.last_cancel_ts_us.get(&key).copied(),
            now_us,
            state.tlen_cancel_freq_ms,
        );
        if !should_emit {
            debug!(
                "MmDecision: MMCancel throttled symbol={} side={} tlen_cancel_freq_ms={}",
                key,
                side.as_str(),
                state.tlen_cancel_freq_ms
            );
        }
        !should_emit
    }

    fn mark_cancel_sent(&mut self, symbol: &str, side: Side, now_us: i64) {
        let key = Self::cancel_throttle_key(symbol, side);
        self.last_cancel_ts_us.insert(key, now_us);
    }

    fn emit_for_symbol(
        &mut self,
        state: &mut MmDecisionState,
        symbol: &str,
        return_score_value: f64,
        return_qtl: Option<f64>,
        thresholds: ReturnScoreCancelThresholds,
        now_us: i64,
    ) -> Result<Option<SignalType>> {
        let Some(sell_cancel_threshold) = thresholds.sell_cancel_score_threshold else {
            return Ok(None);
        };
        let Some(buy_cancel_threshold) = thresholds.buy_cancel_score_threshold else {
            return Ok(None);
        };
        let (sell_cancel_hit, buy_cancel_hit) =
            return_score_cancel_hits(return_score_value, thresholds);
        if !sell_cancel_hit && !buy_cancel_hit {
            return Ok(None);
        }
        let return_qtl_pct = return_qtl.map(|value| value * 100.0);

        let open_quote = match MktChannel::instance().get_quote(symbol, state.open_venue) {
            Some(quote) => quote,
            None => return Ok(None),
        };
        let factor_lookup = state
            .factor_value_hub
            .lookup_factor_value(symbol, state.hedge_venue);
        let volatility = factor_lookup.target_factor_value.filter(|v| v.is_finite());
        let symbol_key = normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures);
        let environment_signal = state.evaluate_environment_signal(&symbol_key, symbol, now_us);
        let mut cancel_sent = false;
        let mut buy_cancel_sent = false;
        let mut sell_cancel_sent = false;
        if sell_cancel_hit {
            if self.cancel_throttled(symbol, Side::Sell, state, now_us) {
                debug!(
                    "MmDecision: skip sell MMCancel due to throttle symbol={} score={:.6} qtl_pct={:?}",
                    symbol_key, return_score_value, return_qtl_pct
                );
            } else {
                let from_key = build_mm_cancel_from_key(
                    now_us,
                    return_qtl,
                    Some(state.return_score_buy_cancel_quantile),
                    volatility,
                    &environment_signal,
                    None,
                    None,
                );
                state.emit_mm_cancel_signal(symbol, Side::Sell, open_quote, now_us, &from_key)?;
                self.mark_cancel_sent(symbol, Side::Sell, now_us);
                cancel_sent = true;
                sell_cancel_sent = true;
            }
        }

        if buy_cancel_hit {
            if self.cancel_throttled(symbol, Side::Buy, state, now_us) {
                debug!(
                    "MmDecision: skip buy MMCancel due to throttle symbol={} score={:.6} qtl_pct={:?}",
                    symbol_key, return_score_value, return_qtl_pct
                );
            } else {
                let from_key = build_mm_cancel_from_key(
                    now_us,
                    return_qtl,
                    Some(state.return_score_sell_cancel_quantile),
                    volatility,
                    &environment_signal,
                    None,
                    None,
                );
                state.emit_mm_cancel_signal(symbol, Side::Buy, open_quote, now_us, &from_key)?;
                self.mark_cancel_sent(symbol, Side::Buy, now_us);
                cancel_sent = true;
                buy_cancel_sent = true;
            }
        }

        if cancel_sent {
            let side_text = match (buy_cancel_sent, sell_cancel_sent) {
                (true, true) => "both",
                (true, false) => "buy",
                (false, true) => "sell",
                (false, false) => "-",
            };
            debug!(
                "MmDecision: MMCancel symbol={} side={} rolling_score={:.6} qtl_pct={:?} sell_cancel_threshold={:.6} buy_cancel_threshold={:.6} sell_threshold_qtl={:.2} buy_threshold_qtl={:.2}",
                symbol_key,
                side_text,
                return_score_value,
                return_qtl_pct,
                sell_cancel_threshold,
                buy_cancel_threshold,
                state.return_score_buy_cancel_quantile,
                state.return_score_sell_cancel_quantile,
            );
            Ok(Some(SignalType::MMCancel))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn process_return_score_updates(
        &mut self,
        state: &mut MmDecisionState,
        events: Vec<ModelOutputUpdateEvent>,
    ) {
        if !state.enable_return_score_cancel {
            return;
        }

        let Some(service_name) = state.return_model_service.clone() else {
            return;
        };

        let online_symbols = SymbolList::instance().get_online_symbols();
        if online_symbols.is_empty() {
            return;
        }

        let online_set: HashMap<String, String> = online_symbols
            .into_iter()
            .map(|symbol| {
                (
                    normalize_symbol_for_whitelist(&symbol, TradingVenue::OkexFutures),
                    symbol,
                )
            })
            .collect();

        let now_us = get_timestamp_us();
        for event in events {
            if event.service_name != service_name || !event.score.is_finite() || !event.score_ready
            {
                continue;
            }
            let symbol_key =
                normalize_symbol_for_whitelist(&event.symbol_key, TradingVenue::OkexFutures);
            let Some(symbol) = online_set.get(&symbol_key) else {
                continue;
            };
            let Some(thresholds) = state
                .return_score_cancel_thresholds
                .get(&symbol_key)
                .copied()
            else {
                continue;
            };
            let return_qtl = event.score_quantile.filter(|value| value.is_finite());
            if let Err(err) =
                self.emit_for_symbol(state, symbol, event.score, return_qtl, thresholds, now_us)
            {
                warn!(
                    "MmDecision: MMCancel evaluate failed symbol={} err={:#}",
                    symbol_key, err
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_emit_allows_first_cancel() {
        assert!(MmCancelDecision::should_emit(None, 1_000_000, 3_000));
    }

    #[test]
    fn should_emit_blocks_within_frequency_window() {
        assert!(!MmCancelDecision::should_emit(
            Some(1_000_000),
            3_999_999,
            3_000
        ));
    }

    #[test]
    fn should_emit_allows_after_frequency_window() {
        assert!(MmCancelDecision::should_emit(
            Some(1_000_000),
            4_000_000,
            3_000
        ));
    }

    #[test]
    fn cancel_throttle_key_is_side_specific() {
        let buy_key = MmCancelDecision::cancel_throttle_key("BTCUSDT", Side::Buy);
        let sell_key = MmCancelDecision::cancel_throttle_key("BTCUSDT", Side::Sell);
        assert_ne!(buy_key, sell_key);
    }

    #[test]
    fn rolling_score_thresholds_are_side_specific() {
        let thresholds = ReturnScoreCancelThresholds {
            sell_cancel_score_threshold: Some(0.8),
            buy_cancel_score_threshold: Some(0.2),
        };
        assert_eq!(return_score_cancel_hits(0.9, thresholds), (true, false));
        assert_eq!(return_score_cancel_hits(0.1, thresholds), (false, true));
        assert_eq!(return_score_cancel_hits(0.5, thresholds), (false, false));
    }
}
