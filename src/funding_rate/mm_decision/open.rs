use anyhow::Result;
use log::{info, warn};
use std::collections::HashMap;

use super::super::factor_value_hub::EnvironmentSignalResult;
use super::super::inline_volatility::INLINE_VOLATILITY_MIN_SAMPLES;
use super::super::mkt_channel::MktChannel;
use super::super::symbol_list::SymbolList;
use super::from_key::{build_from_key, select_prediction_side};
use super::state::{MmDecisionState, MmOpenPublishStats};
use crate::common::time_util::get_timestamp_us;
use crate::market_maker::open_quote_plan::build_mm_open_quote_plan;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::SignalType;
use crate::symbol_match::normalize_symbol_for_whitelist;

pub(crate) struct MmOpenDecision {
    last_eval_ts_us: HashMap<String, i64>,
}

fn mm_open_blocked_by_environment(
    enable_environment_model: bool,
    environment_signal: &EnvironmentSignalResult,
) -> bool {
    enable_environment_model && !environment_signal.allow_open
}

fn resolve_mm_open_return_score(
    prediction_mode: bool,
    score: Option<f64>,
    service_name: &str,
    note: &str,
) -> Result<f64> {
    if !prediction_mode {
        return Ok(0.0);
    }
    score.filter(|v| v.is_finite()).ok_or_else(|| {
        anyhow::anyhow!(
            "return_score unavailable service={} note={}",
            service_name,
            note
        )
    })
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
    ) -> Result<MmOpenEvalResult> {
        let symbol_key = normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures);
        let open_quote = match MktChannel::instance().get_quote(symbol, state.open_venue) {
            Some(quote) => quote,
            None => {
                return Ok(MmOpenEvalResult::skipped(
                    &symbol_key,
                    "missing_open_quote",
                    None,
                    None,
                    None,
                ))
            }
        };

        let volatility_lookup = state
            .factor_value_hub
            .lookup_factor_value(symbol, state.hedge_venue);
        let Some(volatility) = volatility_lookup
            .target_factor_value
            .filter(|v| v.is_finite())
        else {
            return Ok(MmOpenEvalResult::skipped(
                &symbol_key,
                &format!("missing_volatility({})", volatility_lookup.note),
                None,
                None,
                None,
            ));
        };
        let open_volatility_snapshot = state.observe_open_volatility(&symbol_key, now_us / 1000, volatility);
        if state.enable_volatility_limit {
            let Some(open_volatility_threshold) = open_volatility_snapshot.threshold else {
                return Ok(MmOpenEvalResult::skipped(
                    &symbol_key,
                    &format!(
                        "volatility_threshold_warming_up(samples={} min_samples={} percentile={:.2})",
                        open_volatility_snapshot.sample_count,
                        INLINE_VOLATILITY_MIN_SAMPLES,
                        open_volatility_snapshot.percentile
                    ),
                    None,
                    Some(volatility),
                    None,
                ));
            };
            if volatility > open_volatility_threshold {
                warn!(
                    "MmDecision: MMOpen blocked by inline volatility threshold symbol={} current={:.8} threshold={:.8} samples={} percentile={:.2} last_recompute_tp_ms={:?}",
                    symbol_key,
                    open_volatility_snapshot.current,
                    open_volatility_threshold,
                    open_volatility_snapshot.sample_count,
                    open_volatility_snapshot.percentile,
                    open_volatility_snapshot.last_recompute_tp_ms
                );
                return Ok(MmOpenEvalResult::skipped(
                    &symbol_key,
                    &format!(
                        "volatility_limited(current={:.8}>threshold={:.8},samples={},percentile={:.2},last_recompute_tp_ms={})",
                        open_volatility_snapshot.current,
                        open_volatility_threshold,
                        open_volatility_snapshot.sample_count,
                        open_volatility_snapshot.percentile,
                        open_volatility_snapshot.last_recompute_tp_ms.unwrap_or_default()
                    ),
                    None,
                    Some(volatility),
                    None,
                ));
            }
        }

        let (return_score, thresholds, return_qtl) = if state.prediction_mode {
            let Some(service_name) = state.return_model_service.clone() else {
                return Ok(MmOpenEvalResult::skipped(
                    &symbol_key,
                    "missing_return_model_service",
                    None,
                    Some(volatility),
                    None,
                ));
            };
            let score_lookup = state.factor_value_hub.cached_model_output_score(
                &service_name,
                symbol,
                state.hedge_venue,
            );
            let return_qtl = score_lookup.score_quantile.filter(|v| v.is_finite());
            let return_score = match resolve_mm_open_return_score(
                state.prediction_mode,
                score_lookup.score,
                &service_name,
                &score_lookup.note,
            ) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(MmOpenEvalResult::skipped(
                        &symbol_key,
                        &format!("missing_return_score({})", score_lookup.note),
                        None,
                        Some(volatility),
                        None,
                    ))
                }
            };
            let threshold_symbol = score_lookup.symbol_key.to_ascii_uppercase();
            let thresholds = state
                .return_score_thresholds
                .get(&threshold_symbol)
                .copied();
            (return_score, thresholds, return_qtl)
        } else {
            (0.0, None, None)
        };
        if state.prediction_mode && thresholds.is_none() {
            return Ok(MmOpenEvalResult::skipped(
                &symbol_key,
                "missing_return_thresholds(prediction_mode=true)",
                Some(return_score),
                Some(volatility),
                None,
            ));
        }

        let (
            prediction_side,
            open_return_threshold,
            _forward_open_hit,
            _backward_open_hit,
            prediction_ready,
        ) = select_prediction_side(state.prediction_mode, Some(return_score), thresholds);
        if state.prediction_mode && !prediction_ready {
            return Ok(MmOpenEvalResult::skipped(
                &symbol_key,
                "prediction_not_ready(score_not_hit_open_threshold)",
                Some(return_score),
                Some(volatility),
                None,
            ));
        }

        let environment_signal = state.evaluate_environment_signal(&symbol_key, symbol, now_us);
        if mm_open_blocked_by_environment(state.enable_environment_model, &environment_signal) {
            return Ok(MmOpenEvalResult::skipped(
                &symbol_key,
                &format!("environment_blocked({})", environment_signal.note),
                Some(return_score),
                Some(volatility),
                Some(environment_signal),
            ));
        }
        let from_key = build_from_key(
            now_us,
            return_qtl,
            open_return_threshold,
            Some(volatility),
            &environment_signal,
        );

        let plan = match build_mm_open_quote_plan(
            state.open_venue,
            symbol,
            open_quote,
            state.resolve_order_amount_u(symbol),
            state.open_orders_per_round,
            state.open_order_ttl_us,
            volatility,
            state.open_buy_vol_scale,
            state.open_sell_vol_scale,
            now_us,
            &state.open_min_qty_table,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                warn!(
                    "MmDecision: build open quote plan failed symbol={} err={}",
                    symbol_key, err
                );
                return Ok(MmOpenEvalResult::skipped(
                    &symbol_key,
                    &format!("build_plan_failed({err})"),
                    Some(return_score),
                    Some(volatility),
                    Some(environment_signal),
                ));
            }
        };

        let publish_stats = state.publish_mm_open_plan(now_us, &plan, &from_key, prediction_side);

        if publish_stats.sent > 0 {
            let side_text = match prediction_side {
                Some(side) => side.as_str(),
                None => "both",
            };
            info!(
                "MmDecision: MMOpen symbol={} side={} buy={} sell={} score={:.6}",
                symbol_key,
                side_text,
                publish_stats.sent_buy,
                publish_stats.sent_sell,
                return_score,
            );
            Ok(MmOpenEvalResult::emitted(
                &symbol_key,
                &format!(
                    "emitted(sent={} buy={} sell={})",
                    publish_stats.sent, publish_stats.sent_buy, publish_stats.sent_sell
                ),
                Some(return_score),
                Some(volatility),
                Some(environment_signal),
                SignalType::MMOpen,
            ))
        } else {
            Ok(MmOpenEvalResult::skipped(
                &symbol_key,
                &publish_failure_reason(&publish_stats),
                Some(return_score),
                Some(volatility),
                Some(environment_signal),
            ))
        }
    }

    pub(crate) fn process_interval(&mut self, state: &mut MmDecisionState) {
        let now_us = get_timestamp_us();
        let mut results = Vec::new();
        for symbol in SymbolList::instance().get_online_symbols() {
            let symbol_key = normalize_symbol_for_whitelist(&symbol, TradingVenue::OkexFutures);
            if !self.should_run_for_symbol(state, &symbol_key, now_us) {
                continue;
            }
            self.mark_evaluated(&symbol_key, now_us);
            match self.emit_for_symbol(state, &symbol, now_us) {
                Ok(result) => results.push(result),
                Err(err) => {
                    warn!(
                        "MmDecision: MMOpen evaluate failed symbol={} err={:#}",
                        symbol_key, err
                    );
                    results.push(MmOpenEvalResult::skipped(
                        &symbol_key,
                        &format!("evaluate_failed({})", err.root_cause()),
                        None,
                        None,
                        None,
                    ));
                }
            }
        }

        log_interval_summary(state, &results);
    }
}

#[derive(Debug, Clone)]
struct MmOpenEvalResult {
    symbol: String,
    result: &'static str,
    reason: String,
    return_score: Option<f64>,
    volatility: Option<f64>,
    vol_tr: Option<f64>,
    env_note: String,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    signal_type: Option<SignalType>,
}

impl MmOpenEvalResult {
    fn skipped(
        symbol: &str,
        reason: &str,
        return_score: Option<f64>,
        volatility: Option<f64>,
        env: Option<EnvironmentSignalResult>,
    ) -> Self {
        let (env_note, env_score, env_threshold) = env_fields(env.as_ref());
        Self {
            symbol: symbol.to_string(),
            result: "skip",
            reason: reason.to_string(),
            return_score,
            volatility,
            vol_tr: extract_vol_tr(reason),
            env_note,
            env_score,
            env_threshold,
            signal_type: None,
        }
    }

    fn emitted(
        symbol: &str,
        reason: &str,
        return_score: Option<f64>,
        volatility: Option<f64>,
        env: Option<EnvironmentSignalResult>,
        signal_type: SignalType,
    ) -> Self {
        let (env_note, env_score, env_threshold) = env_fields(env.as_ref());
        Self {
            symbol: symbol.to_string(),
            result: "emit",
            reason: reason.to_string(),
            return_score,
            volatility,
            vol_tr: extract_vol_tr(reason),
            env_note,
            env_score,
            env_threshold,
            signal_type: Some(signal_type),
        }
    }
}

fn extract_vol_tr(reason: &str) -> Option<f64> {
    let key = "threshold=";
    let start = reason.find(key)? + key.len();
    let rest = &reason[start..];
    let end = rest.find([',', ')']).unwrap_or(rest.len());
    rest[..end].trim().parse::<f64>().ok()
}

fn env_fields(env: Option<&EnvironmentSignalResult>) -> (String, Option<f64>, Option<f64>) {
    match env {
        Some(value) => (value.note.clone(), value.score, value.threshold),
        None => ("-".to_string(), None, None),
    }
}

fn publish_failure_reason(stats: &MmOpenPublishStats) -> String {
    if stats.prepared_levels == 0 {
        if stats.tlen_filtered_levels > 0 {
            format!(
                "all_levels_filtered_by_tlen_or_quantization(zero_quantized={} tlen_filtered={})",
                stats.zero_quantized_levels, stats.tlen_filtered_levels
            )
        } else {
            format!(
                "all_levels_filtered_zero_qty_or_price(zero_quantized={})",
                stats.zero_quantized_levels
            )
        }
    } else if stats.publish_failures >= stats.prepared_levels {
        format!(
            "publish_failed_all(prepared={} failures={})",
            stats.prepared_levels, stats.publish_failures
        )
    } else {
        format!(
            "no_level_emitted(prepared={} zero_quantized={} tlen_filtered={} failures={})",
            stats.prepared_levels,
            stats.zero_quantized_levels,
            stats.tlen_filtered_levels,
            stats.publish_failures
        )
    }
}

fn format_opt_f64(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.6}"),
        _ => "-".to_string(),
    }
}

fn format_env_cell(item: &MmOpenEvalResult) -> String {
    if item.env_note == "-" {
        return "-".to_string();
    }
    let score = format_opt_f64(item.env_score);
    let threshold = format_opt_f64(item.env_threshold);
    format!("{} ({}/{})", item.env_note, score, threshold)
}

fn truncate_cell(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() && max_chars > 0 {
        let keep = max_chars.saturating_sub(1);
        let head: String = value.chars().take(keep).collect();
        format!("{head}…")
    } else {
        truncated
    }
}

fn pad_cell(value: &str, width: usize) -> String {
    let clipped = truncate_cell(value, width);
    format!("{clipped:<width$}")
}

fn build_rule(widths: &[usize], left: char, mid: char, right: char) -> String {
    let mut out = String::new();
    out.push(left);
    for (idx, width) in widths.iter().enumerate() {
        out.push_str(&"─".repeat(width.saturating_add(2)));
        if idx + 1 == widths.len() {
            out.push(right);
        } else {
            out.push(mid);
        }
    }
    out
}

fn format_mm_open_eval_table(results: &[MmOpenEvalResult]) -> String {
    let widths = [14usize, 6, 10, 10, 10, 34, 44];
    let headers = ["symbol", "result", "ret", "vol", "vol_tr", "env", "reason"];
    let mut lines = Vec::new();
    lines.push(build_rule(&widths, '┌', '┬', '┐'));
    let header_cells: Vec<String> = headers
        .iter()
        .zip(widths.iter())
        .map(|(header, width)| pad_cell(header, *width))
        .collect();
    lines.push(format!(
        "│ {} │ {} │ {} │ {} │ {} │ {} │ {} │",
        header_cells[0],
        header_cells[1],
        header_cells[2],
        header_cells[3],
        header_cells[4],
        header_cells[5],
        header_cells[6],
    ));
    lines.push(build_rule(&widths, '├', '┼', '┤'));
    for item in results {
        let signal_hint = match item.signal_type {
            Some(SignalType::MMOpen) => "emit",
            _ => item.result,
        };
        lines.push(format!(
            "│ {} │ {} │ {} │ {} │ {} │ {} │ {} │",
            pad_cell(&item.symbol, widths[0]),
            pad_cell(signal_hint, widths[1]),
            pad_cell(&format_opt_f64(item.return_score), widths[2]),
            pad_cell(&format_opt_f64(item.volatility), widths[3]),
            pad_cell(&format_opt_f64(item.vol_tr), widths[4]),
            pad_cell(&format_env_cell(item), widths[5]),
            pad_cell(&item.reason, widths[6]),
        ));
    }
    lines.push(build_rule(&widths, '└', '┴', '┘'));
    lines.join("\n")
}

fn log_interval_summary(state: &MmDecisionState, results: &[MmOpenEvalResult]) {
    if results.is_empty() {
        return;
    }

    let evaluated = results.len();
    let emitted = results.iter().filter(|item| item.result == "emit").count();
    let skipped = evaluated.saturating_sub(emitted);
    let emitted_orders = results.iter().filter(|item| item.result == "emit").count();
    info!(
        "MmDecision: MMOpen interval summary interval_ms={} prediction_mode={} evaluated={} emitted_symbols={} skipped_symbols={} emitted_orders={} thresholds_required={}",
        state.order_interval_ms,
        state.prediction_mode,
        evaluated,
        emitted,
        skipped,
        emitted_orders,
        state.prediction_mode
    );

    let max_rows = 16usize;
    let shown = results.len().min(max_rows);
    info!(
        "MmDecision: MMOpen interval detail table{}\n{}",
        if results.len() > max_rows {
            format!(" (showing first {} of {})", shown, results.len())
        } else {
            String::new()
        },
        format_mm_open_eval_table(&results[..shown])
    );
}

#[cfg(test)]
mod tests {
    use super::{mm_open_blocked_by_environment, resolve_mm_open_return_score};
    use crate::funding_rate::factor_value_hub::{EnvironmentSignalResult, EnvironmentSignalSource};

    fn sample_environment_signal(allow_open: bool) -> EnvironmentSignalResult {
        EnvironmentSignalResult {
            source: EnvironmentSignalSource::PnluFallback,
            allow_open,
            class_label: if allow_open { 1 } else { 0 },
            service_name: None,
            symbol_key: "DOGEUSDT".to_string(),
            score: Some(0.00216925),
            score_quantile: None,
            threshold: Some(0.00234964),
            note: "pnlu_fallback:test".to_string(),
        }
    }

    #[test]
    fn mm_open_is_blocked_when_environment_disallows_open() {
        assert!(mm_open_blocked_by_environment(
            true,
            &sample_environment_signal(false)
        ));
    }

    #[test]
    fn mm_open_is_allowed_when_environment_allows_open() {
        assert!(!mm_open_blocked_by_environment(
            true,
            &sample_environment_signal(true)
        ));
    }

    #[test]
    fn mm_open_ignores_environment_block_when_disabled() {
        assert!(!mm_open_blocked_by_environment(
            false,
            &sample_environment_signal(false)
        ));
    }

    #[test]
    fn prediction_mode_disabled_uses_zero_return_score() {
        assert_eq!(
            resolve_mm_open_return_score(false, None, "model_output/test", "missing").unwrap(),
            0.0
        );
        assert_eq!(
            resolve_mm_open_return_score(false, Some(0.42), "model_output/test", "present")
                .unwrap(),
            0.0
        );
    }

    #[test]
    fn prediction_mode_enabled_still_requires_return_score() {
        assert!(
            resolve_mm_open_return_score(true, None, "model_output/test", "missing")
                .unwrap_err()
                .to_string()
                .contains("return_score unavailable")
        );
    }
}
