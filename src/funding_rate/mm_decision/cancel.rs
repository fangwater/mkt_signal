use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;

use super::super::mkt_channel::MktChannel;
use super::super::symbol_list::SymbolList;
use super::from_key::build_from_key;
use super::state::MmDecisionState;
use crate::common::time_util::get_timestamp_us;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::SignalType;
use crate::symbol_match::normalize_symbol_for_whitelist;

pub(crate) struct MmCancelDecision;

impl MmCancelDecision {
    pub(crate) fn new() -> Self {
        Self
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
        if forward_cancel_hit {
            let from_key = build_from_key(
                now_us,
                Some(return_score_value),
                Some(thresholds.forward_cancel),
                volatility,
                &environment_signal,
            );
            state.emit_mm_cancel_signal(
                symbol,
                crate::pre_trade::order_manager::Side::Buy,
                open_quote,
                now_us,
                &from_key,
            )?;
            cancel_sent = true;
        }

        if backward_cancel_hit {
            let from_key = build_from_key(
                now_us,
                Some(return_score_value),
                Some(thresholds.backward_cancel),
                volatility,
                &environment_signal,
            );
            state.emit_mm_cancel_signal(
                symbol,
                crate::pre_trade::order_manager::Side::Sell,
                open_quote,
                now_us,
                &from_key,
            )?;
            cancel_sent = true;
        }

        if cancel_sent {
            let side_text = match (forward_cancel_hit, backward_cancel_hit) {
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
