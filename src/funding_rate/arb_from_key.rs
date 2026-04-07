use crate::signal::common::TradingVenue;

use super::common::{
    append_key_value_fields, append_tlen_suffix, build_open_from_key_base,
    format_from_key_optional_value,
};

pub fn build_spread_decision_from_key_base(
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    open_scale: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    spread_rate: f64,
    spread_fr: Option<f64>,
) -> String {
    let base = build_open_from_key_base(
        now,
        return_score,
        return_threshold,
        volatility,
        open_scale,
        env_score,
        env_threshold,
        spread_rate,
    );
    append_key_value_fields(
        base,
        &[("spread_fr", format_from_key_optional_value(spread_fr, 6))],
    )
}
pub fn build_funding_decision_from_key_base(
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    open_scale: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    futures_symbol: &str,
    futures_venue: TradingVenue,
    spread_rate: f64,
    premium_rate: Option<f64>,
) -> String {
    let _ = (futures_symbol, futures_venue);
    let base = build_open_from_key_base(
        now,
        return_score,
        return_threshold,
        volatility,
        open_scale,
        env_score,
        env_threshold,
        spread_rate,
    );
    append_key_value_fields(
        base,
        &[(
            "premium_rate",
            format_from_key_optional_value(premium_rate, 6),
        )],
    )
}

pub fn build_funding_decision_from_key(
    now: i64,
    futures_symbol: &str,
    futures_venue: TradingVenue,
    spread_rate: f64,
    premium_rate: Option<f64>,
) -> Vec<u8> {
    build_funding_decision_from_key_base(
        now,
        None,
        None,
        None,
        None,
        None,
        None,
        futures_symbol,
        futures_venue,
        spread_rate,
        premium_rate,
    )
    .into_bytes()
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
    premium_rate: Option<f64>,
    open_scale: Option<f64>,
) -> Vec<u8> {
    let base = build_funding_decision_from_key_base(
        now,
        return_score,
        return_threshold,
        volatility,
        open_scale,
        env_score,
        env_threshold,
        futures_symbol,
        futures_venue,
        spread_rate,
        premium_rate,
    );
    base.into_bytes()
}

pub fn build_spread_arb_cancel_from_key(
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    volatility: Option<f64>,
    open_scale: Option<f64>,
    spread_rate: f64,
    spread_fr: Option<f64>,
) -> String {
    build_spread_decision_from_key_base(
        now,
        return_score,
        return_threshold,
        volatility,
        open_scale,
        Some(environment_score),
        environment_threshold,
        spread_rate,
        spread_fr,
    )
}

pub fn build_spread_arb_tlen_cancel_from_key(
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    volatility: Option<f64>,
    open_scale: Option<f64>,
    spread_rate: f64,
    spread_fr: Option<f64>,
    tlen: f64,
    threshold: f64,
) -> String {
    let from_key = build_spread_arb_cancel_from_key(
        now,
        return_score,
        return_threshold,
        environment_score,
        environment_threshold,
        volatility,
        open_scale,
        spread_rate,
        spread_fr,
    );
    append_tlen_suffix(from_key, tlen, threshold)
}

pub fn build_funding_tlen_cancel_from_key(
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    open_scale: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    spread_rate: f64,
    premium_rate: Option<f64>,
    tlen: f64,
    threshold: f64,
) -> String {
    append_tlen_suffix(
        build_funding_decision_from_key_base(
            now,
            return_score,
            return_threshold,
            volatility,
            open_scale,
            env_score,
            env_threshold,
            "",
            TradingVenue::OkexFutures,
            spread_rate,
            premium_rate,
        ),
        tlen,
        threshold,
    )
}
