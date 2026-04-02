use crate::signal::common::TradingVenue;

use super::common::build_decision_from_key_base;
use super::mkt_channel::MktChannel;
use super::rate_fetcher::RateFetcher;

pub fn build_funding_decision_from_key(
    now: i64,
    futures_symbol: &str,
    futures_venue: TradingVenue,
    spread_rate: f64,
) -> Vec<u8> {
    let mkt_channel = MktChannel::instance();
    let rate_fetcher = RateFetcher::instance();
    let funding_ma = mkt_channel
        .get_funding_rate_mean(futures_symbol, futures_venue)
        .unwrap_or(0.0);
    let predicted = rate_fetcher
        .get_predicted_funding_rate(futures_symbol, futures_venue)
        .map(|(_, v)| v)
        .unwrap_or(0.0);
    let loan = rate_fetcher
        .get_predict_loan_rate(futures_symbol, futures_venue)
        .map(|(_, v)| v)
        .unwrap_or(0.0);

    format!("{now}:{funding_ma:.6}:{predicted:.6}:{loan:.6}:{spread_rate:.6}").into_bytes()
}

pub fn build_funding_decision_from_key_with_gate(
    now: i64,
    futures_symbol: &str,
    futures_venue: TradingVenue,
    spread_rate: f64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    open_filter_value: Option<f64>,
    open_filter_threshold: Option<f64>,
    open_scale: Option<f64>,
) -> Vec<u8> {
    let mkt_channel = MktChannel::instance();
    let rate_fetcher = RateFetcher::instance();
    let funding_ma = mkt_channel
        .get_funding_rate_mean(futures_symbol, futures_venue)
        .unwrap_or(0.0);
    let predicted = rate_fetcher
        .get_predicted_funding_rate(futures_symbol, futures_venue)
        .map(|(_, v)| v)
        .unwrap_or(0.0);
    let loan = rate_fetcher
        .get_predict_loan_rate(futures_symbol, futures_venue)
        .map(|(_, v)| v)
        .unwrap_or(0.0);
    let base = build_decision_from_key_base(
        now,
        return_score,
        return_threshold,
        volatility,
        env_score,
        env_threshold,
    );
    let open_filter_value_text = open_filter_value
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.6}"))
        .unwrap_or_else(|| "NA".to_string());
    let open_filter_threshold_text = open_filter_threshold
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.6}"))
        .unwrap_or_else(|| "NA".to_string());
    let open_scale_text = open_scale
        .filter(|v| v.is_finite())
        .map(|v| format!("{v:.6}"))
        .unwrap_or_else(|| "NA".to_string());

    format!(
        "{base}:funding_ma={funding_ma:.6}:predicted={predicted:.6}:loan={loan:.6}:spread={spread_rate:.6}:open_signal={open_filter_value_text}:open_signal_thr={open_filter_threshold_text}:open_scale={open_scale_text}"
    )
    .into_bytes()
}

pub fn build_spread_arb_cancel_from_key(
    now: i64,
    return_score: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    volatility: Option<f64>,
    spread_rate: f64,
) -> String {
    let from_key = build_decision_from_key_base(
        now,
        return_score,
        None,
        volatility,
        Some(environment_score),
        environment_threshold,
    );
    format!("{from_key}:spread={spread_rate:.6}")
}

pub fn build_spread_arb_tlen_cancel_from_key(
    now: i64,
    return_score: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    volatility: Option<f64>,
    spread_rate: f64,
    tlen: f64,
    threshold: f64,
) -> String {
    let from_key = build_spread_arb_cancel_from_key(
        now,
        return_score,
        environment_score,
        environment_threshold,
        volatility,
        spread_rate,
    );
    format!("{from_key}:cancel_reason=tlen:tlen={tlen:.8}:tlen_thr={threshold:.8}")
}

pub fn build_funding_tlen_cancel_from_key(
    now: i64,
    spread_rate: f64,
    tlen: f64,
    threshold: f64,
) -> String {
    format!(
        "{now}:0:0:0:{spread_rate:.6}:cancel_reason=tlen:tlen={tlen:.8}:tlen_thr={threshold:.8}"
    )
}
