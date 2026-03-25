use anyhow::Result;
use log::{debug, info, warn};
use std::collections::HashMap;

use super::super::mkt_channel::MktChannel;
use super::super::symbol_list::SymbolList;
use super::from_key::build_mm_cancel_from_key;
use super::state::MmDecisionState;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::SignalType;
use crate::symbol_match::normalize_symbol_for_whitelist;

pub(crate) struct MmCancelDecision {
    last_cancel_ts_us: HashMap<String, i64>,
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
        now_us: i64,
    ) -> Result<Option<SignalType>> {
        let threshold_symbol =
            crate::common::symbol_util::normalize_symbol_for_venue(symbol, state.hedge_venue)
                .to_ascii_uppercase();
        let Some(thresholds) = state
            .return_score_thresholds
            .get(&threshold_symbol)
            .copied()
        else {
            return Ok(None);
        };

        let forward_cancel_hit = return_score_value < thresholds.forward_cancel;
        let backward_cancel_hit = return_score_value > thresholds.backward_cancel;
        if !forward_cancel_hit && !backward_cancel_hit {
            return Ok(None);
        }

        let open_quote = match MktChannel::instance().get_quote(symbol, state.open_venue) {
            Some(quote) => quote,
            None => return Ok(None),
        };
        let factor_lookup = state
            .factor_value_hub
            .lookup_target_factor_value(symbol, state.hedge_venue);
        let volatility = factor_lookup.target_factor_value.filter(|v| v.is_finite());
        let symbol_key = normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures);
        let environment_signal = state.evaluate_environment_signal(&symbol_key, symbol, now_us);
        let mut cancel_sent = false;
        let mut forward_cancel_sent = false;
        let mut backward_cancel_sent = false;
        if forward_cancel_hit {
            if self.cancel_throttled(symbol, Side::Buy, state, now_us) {
                debug!(
                    "MmDecision: skip buy MMCancel due to throttle symbol={} score={:.6}",
                    symbol_key, return_score_value
                );
            } else {
                let from_key = build_mm_cancel_from_key(
                    now_us,
                    Some(return_score_value),
                    Some(thresholds.forward_cancel),
                    volatility,
                    &environment_signal,
                    None,
                    None,
                );
                state.emit_mm_cancel_signal(symbol, Side::Buy, open_quote, now_us, &from_key)?;
                self.mark_cancel_sent(symbol, Side::Buy, now_us);
                cancel_sent = true;
                forward_cancel_sent = true;
            }
        }

        if backward_cancel_hit {
            if self.cancel_throttled(symbol, Side::Sell, state, now_us) {
                debug!(
                    "MmDecision: skip sell MMCancel due to throttle symbol={} score={:.6}",
                    symbol_key, return_score_value
                );
            } else {
                let from_key = build_mm_cancel_from_key(
                    now_us,
                    Some(return_score_value),
                    Some(thresholds.backward_cancel),
                    volatility,
                    &environment_signal,
                    None,
                    None,
                );
                state.emit_mm_cancel_signal(symbol, Side::Sell, open_quote, now_us, &from_key)?;
                self.mark_cancel_sent(symbol, Side::Sell, now_us);
                cancel_sent = true;
                backward_cancel_sent = true;
            }
        }

        if cancel_sent {
            let side_text = match (forward_cancel_sent, backward_cancel_sent) {
                (true, true) => "both",
                (true, false) => "buy",
                (false, true) => "sell",
                (false, false) => "-",
            };
            info!(
                "MmDecision: MMCancel symbol={} side={} score={:.6}",
                symbol_key, side_text, return_score_value,
            );
            Ok(Some(SignalType::MMCancel))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn process_return_score_updates(&mut self, state: &mut MmDecisionState) {
        if !state.enable_open_cancel {
            let _ = state.factor_value_hub.poll_model_output_updates();
            return;
        }

        let Some(service_name) = state.return_model_service.clone() else {
            return;
        };

        let online_symbols = SymbolList::instance().get_online_symbols();
        if online_symbols.is_empty() {
            let _ = state.factor_value_hub.poll_model_output_updates();
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
        for event in state.factor_value_hub.poll_model_output_updates() {
            if event.service_name != service_name || !event.score.is_finite() {
                continue;
            }
            let symbol_key =
                normalize_symbol_for_whitelist(&event.symbol_key, TradingVenue::OkexFutures);
            let Some(symbol) = online_set.get(&symbol_key) else {
                continue;
            };
            if let Err(err) = self.emit_for_symbol(state, symbol, event.score, now_us) {
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
}
