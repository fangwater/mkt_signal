use serde::Deserialize;
use std::collections::HashMap;

use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_venue;

const MODEL_SCORE_THRESHOLD_KEY_PREFIX: &str = "model_score_rolling_thresholds_";

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ReturnScoreCancelThresholds {
    pub sell_cancel_score_threshold: Option<f64>,
    pub buy_cancel_score_threshold: Option<f64>,
}

impl ReturnScoreCancelThresholds {
    pub fn ready(&self) -> bool {
        self.sell_cancel_score_threshold.is_some() && self.buy_cancel_score_threshold.is_some()
    }
}

#[derive(Debug)]
pub struct ReturnScoreThresholdLoad {
    pub output_hash_key: String,
    pub fields: usize,
    pub ready_symbols: usize,
    pub cached_symbols: usize,
    pub invalid_payloads: usize,
    pub by_symbol: HashMap<String, ReturnScoreCancelThresholds>,
}

#[derive(Debug, Deserialize)]
struct RawModelScoreThresholdPayload {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    quantiles: Vec<f64>,
    #[serde(default)]
    thresholds: Vec<f64>,
}

pub fn model_score_threshold_output_key(model_service: &str) -> Option<String> {
    let model_name = model_service
        .trim()
        .rsplit('/')
        .find(|part| !part.trim().is_empty())?;
    if model_name == "-" {
        return None;
    }
    Some(format!(
        "{MODEL_SCORE_THRESHOLD_KEY_PREFIX}{}",
        model_name.trim()
    ))
}

pub fn parse_return_score_thresholds(
    output_hash_key: &str,
    raw: &HashMap<String, String>,
    sell_cancel_percentile: f64,
    buy_cancel_percentile: f64,
    venue: TradingVenue,
) -> ReturnScoreThresholdLoad {
    let sell_cancel_key = percentile_key_from_percentile(sell_cancel_percentile);
    let buy_cancel_key = percentile_key_from_percentile(buy_cancel_percentile);
    let mut by_symbol = HashMap::with_capacity(raw.len());
    let mut ready_symbols = 0usize;
    let mut invalid_payloads = 0usize;

    for (field_symbol, text) in raw {
        let payload = match serde_json::from_str::<RawModelScoreThresholdPayload>(text) {
            Ok(payload) => payload,
            Err(_) => {
                invalid_payloads = invalid_payloads.saturating_add(1);
                continue;
            }
        };
        if !payload.ready {
            continue;
        }
        if payload.quantiles.is_empty() || payload.quantiles.len() != payload.thresholds.len() {
            invalid_payloads = invalid_payloads.saturating_add(1);
            continue;
        }

        let raw_symbol = payload.symbol.as_deref().unwrap_or(field_symbol.as_str());
        let symbol = normalize_symbol_for_venue(raw_symbol, venue);
        if symbol.is_empty() {
            invalid_payloads = invalid_payloads.saturating_add(1);
            continue;
        }

        let mut points = HashMap::with_capacity(payload.quantiles.len());
        for (quantile, threshold) in payload.quantiles.iter().zip(payload.thresholds.iter()) {
            let Some(key) = percentile_key_from_quantile(*quantile) else {
                continue;
            };
            if threshold.is_finite() {
                points.insert(key, *threshold);
            }
        }
        if points.is_empty() {
            invalid_payloads = invalid_payloads.saturating_add(1);
            continue;
        }
        ready_symbols = ready_symbols.saturating_add(1);

        let thresholds = ReturnScoreCancelThresholds {
            sell_cancel_score_threshold: sell_cancel_key.and_then(|key| points.get(&key).copied()),
            buy_cancel_score_threshold: buy_cancel_key.and_then(|key| points.get(&key).copied()),
        };
        if thresholds.ready() {
            by_symbol.insert(symbol, thresholds);
        }
    }

    ReturnScoreThresholdLoad {
        output_hash_key: output_hash_key.to_string(),
        fields: raw.len(),
        ready_symbols,
        cached_symbols: by_symbol.len(),
        invalid_payloads,
        by_symbol,
    }
}

fn percentile_key_from_quantile(raw: f64) -> Option<u16> {
    let percentile = if raw.is_finite() && raw <= 1.0 {
        raw * 100.0
    } else {
        raw
    };
    percentile_key_from_percentile(percentile)
}

fn percentile_key_from_percentile(percentile: f64) -> Option<u16> {
    if !(percentile.is_finite() && (0.0..=100.0).contains(&percentile)) {
        return None;
    }
    let key = (percentile * 100.0).round();
    if !(0.0..=10000.0).contains(&key) {
        return None;
    }
    Some(key as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_requested_return_score_thresholds() {
        let raw = HashMap::from([(
            "BTCUSDT".to_string(),
            r#"{
                "symbol":"BTCUSDT",
                "ready":true,
                "quantiles":[0.9,0.8,0.2,0.1],
                "thresholds":[0.9,0.8,0.2,0.1]
            }"#
            .to_string(),
        )]);

        let load = parse_return_score_thresholds(
            "model_score_rolling_thresholds_return_model",
            &raw,
            90.0,
            10.0,
            TradingVenue::BinanceFutures,
        );
        let thresholds = load.by_symbol.get("BTCUSDT").expect("btc thresholds");
        assert_eq!(thresholds.sell_cancel_score_threshold, Some(0.9));
        assert_eq!(thresholds.buy_cancel_score_threshold, Some(0.1));
    }

    #[test]
    fn requires_both_configured_thresholds() {
        let raw = HashMap::from([(
            "BTCUSDT".to_string(),
            r#"{
                "ready":true,
                "quantiles":[0.9],
                "thresholds":[0.9]
            }"#
            .to_string(),
        )]);

        let load = parse_return_score_thresholds(
            "model_score_rolling_thresholds_return_model",
            &raw,
            90.0,
            10.0,
            TradingVenue::BinanceFutures,
        );
        assert!(load.by_symbol.is_empty());
        assert_eq!(load.ready_symbols, 1);
    }
}
