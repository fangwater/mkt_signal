use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;

use super::super::mkt_channel::MktChannel;
use super::super::symbol_list::SymbolList;
use super::from_key::{build_from_key, select_prediction_side};
use super::state::MmDecisionState;
use crate::common::time_util::get_timestamp_us;
use crate::market_maker::open_quote_plan::build_mm_open_quote_plan;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::SignalType;
use crate::symbol_match::normalize_symbol_for_whitelist;

pub(crate) struct MmOpenDecision {
    last_eval_ts_us: HashMap<String, i64>,
}

impl MmOpenDecision {
    pub(crate) fn new() -> Self {
        Self {
            last_eval_ts_us: HashMap::new(),
        }
    }

    fn should_run_for_symbol(
        &self,
        state: &MmDecisionState,
        symbol_key: &str,
        now_us: i64,
    ) -> bool {
        let interval_us = (state.order_interval_ms as i64).saturating_mul(1_000);
        let last_ts = self.last_eval_ts_us.get(symbol_key).copied().unwrap_or(0);
        last_ts == 0 || now_us.saturating_sub(last_ts) >= interval_us
    }

    fn mark_evaluated(&mut self, symbol_key: &str, now_us: i64) {
        self.last_eval_ts_us.insert(symbol_key.to_string(), now_us);
    }

    fn emit_for_symbol(
        &mut self,
        state: &mut MmDecisionState,
        symbol: &str,
        now_us: i64,
    ) -> Result<Option<SignalType>> {
        let symbol_key = normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures);
        let open_quote = match MktChannel::instance().get_quote(symbol, state.open_venue) {
            Some(quote) => quote,
            None => return Ok(None),
        };

        let Some(volatility) = state
            .factor_value_hub
            .lookup_target_factor_value(symbol, state.hedge_venue)
            .target_factor_value
            .filter(|v| v.is_finite())
        else {
            return Ok(None);
        };

        let Some(service_name) = state.return_model_service.clone() else {
            return Ok(None);
        };
        let score_lookup = state.factor_value_hub.cached_model_output_score(
            &service_name,
            symbol,
            state.hedge_venue,
        );
        let Some(return_score) = score_lookup.score.filter(|v| v.is_finite()) else {
            return Ok(None);
        };

        let threshold_symbol = score_lookup.symbol_key.to_ascii_uppercase();
        let thresholds = state
            .return_score_thresholds
            .get(&threshold_symbol)
            .copied();
        if state.prediction_mode && thresholds.is_none() {
            return Ok(None);
        }

        let (
            prediction_side,
            open_return_threshold,
            _forward_open_hit,
            _backward_open_hit,
            prediction_ready,
        ) = select_prediction_side(state.prediction_mode, Some(return_score), thresholds);
        if state.prediction_mode && !prediction_ready {
            return Ok(None);
        }

        let environment_signal = state.evaluate_environment_signal(&symbol_key, symbol, now_us);
        let from_key = build_from_key(
            now_us,
            Some(return_score),
            open_return_threshold,
            Some(volatility),
            &environment_signal,
        );

        let plan = match build_mm_open_quote_plan(
            state.open_venue,
            symbol,
            open_quote,
            state.order_amount_u,
            state.open_orders_per_round,
            state.open_order_ttl_us,
            volatility,
            now_us,
            &state.open_min_qty_table,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                warn!(
                    "MmDecision: build open quote plan failed symbol={} err={}",
                    symbol_key, err
                );
                return Ok(None);
            }
        };

        let (sent, sent_buy, sent_sell) =
            state.publish_mm_open_plan(now_us, &plan, &from_key, prediction_side);

        if sent > 0 {
            let side_text = match prediction_side {
                Some(side) => side.as_str(),
                None => "both",
            };
            info!(
                "MmDecision: MMOpen symbol={} side={} buy={} sell={} score={:.6}",
                symbol_key, side_text, sent_buy, sent_sell, return_score,
            );
            Ok(Some(SignalType::MMOpen))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn process_interval(&mut self, state: &mut MmDecisionState) {
        let now_us = get_timestamp_us();
        for symbol in SymbolList::instance().get_online_symbols() {
            let symbol_key = normalize_symbol_for_whitelist(&symbol, TradingVenue::OkexFutures);
            if !self.should_run_for_symbol(state, &symbol_key, now_us) {
                continue;
            }
            self.mark_evaluated(&symbol_key, now_us);
            if let Err(err) = self.emit_for_symbol(state, &symbol, now_us) {
                warn!(
                    "MmDecision: MMOpen evaluate failed symbol={} err={:#}",
                    symbol_key, err
                );
            }
        }
    }
}
