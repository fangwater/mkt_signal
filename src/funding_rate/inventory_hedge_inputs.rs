use log::warn;
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::funding_rate::factor_value_hub::FactorValueHub;
use crate::funding_rate::model_output_hub::ModelOutputHub;
use order_common::TradingVenue;

const INVENTORY_HEDGE_NEUTRAL_SIGNAL: f64 = 0.0;
const INVENTORY_HEDGE_NEUTRAL_SIGNAL_QUANTILE: f64 = 0.5;
const MISSING_HEDGE_SCORE_LOG_INTERVAL_SECS: u64 = 30;

thread_local! {
    static INVENTORY_HEDGE_MISSING_SCORE_LAST_LOG_AT: RefCell<HashMap<String, Instant>> =
        RefCell::new(HashMap::new());
}

pub fn resolve_inventory_hedge_signal_inputs(
    factor_value_hub: &mut FactorValueHub,
    model_output_hub: &mut ModelOutputHub,
    model_service: &str,
    symbol: &str,
    venue: TradingVenue,
    enable_return_score_adjust_hedge: bool,
) -> Result<(f64, Option<f64>, f64), String> {
    let score_lookup = model_output_hub.lookup_score(model_service, symbol, venue);
    let factor_lookup =
        factor_value_hub.lookup_factor_value_with_last_valid_fallback(symbol, venue);
    let volatility = factor_lookup
        .target_factor_value
        .filter(|v| v.is_finite())
        .ok_or_else(|| {
            format!(
                "missing or invalid volatility factor key={} note={}",
                factor_lookup.key, factor_lookup.note
            )
        })?;
    let signal = if enable_return_score_adjust_hedge {
        if score_lookup.score.filter(|v| v.is_finite()).is_none()
            && should_log_missing_hedge_score(
                symbol,
                &score_lookup.service_name,
                &score_lookup.note,
            )
        {
            warn!(
                "InventoryHedge missing return_score, fallback to neutral symbol={} venue={:?} service={} note={} volatility={:.8} signal_qtl={:.2}",
                symbol,
                venue,
                score_lookup.service_name,
                score_lookup.note,
                volatility,
                INVENTORY_HEDGE_NEUTRAL_SIGNAL_QUANTILE
            );
        }
        resolve_inventory_hedge_effective_signal(
            enable_return_score_adjust_hedge,
            score_lookup.score,
            Some(volatility),
            &score_lookup.service_name,
            &score_lookup.note,
        )?
    } else {
        INVENTORY_HEDGE_NEUTRAL_SIGNAL
    };
    let signal_qtl = resolve_inventory_hedge_signal_quantile(
        enable_return_score_adjust_hedge,
        score_lookup.score,
        score_lookup.score_quantile,
        Some(volatility),
    );
    Ok((signal, signal_qtl, volatility))
}

fn should_log_missing_hedge_score(symbol: &str, service_name: &str, note: &str) -> bool {
    let now = Instant::now();
    let key = format!("{symbol}|{service_name}|{note}");
    INVENTORY_HEDGE_MISSING_SCORE_LAST_LOG_AT.with(|last_log_at| {
        let mut last_log_at = last_log_at.borrow_mut();
        match last_log_at.get(&key) {
            Some(last)
                if now.duration_since(*last)
                    < Duration::from_secs(MISSING_HEDGE_SCORE_LOG_INTERVAL_SECS) =>
            {
                false
            }
            _ => {
                last_log_at.insert(key, now);
                true
            }
        }
    })
}

fn resolve_inventory_hedge_effective_signal(
    enable_return_score_adjust_hedge: bool,
    score: Option<f64>,
    volatility: Option<f64>,
    service_name: &str,
    note: &str,
) -> Result<f64, String> {
    if !enable_return_score_adjust_hedge {
        return Ok(INVENTORY_HEDGE_NEUTRAL_SIGNAL);
    }
    if volatility.filter(|v| v.is_finite()).is_some() && score.filter(|v| v.is_finite()).is_none() {
        return Ok(INVENTORY_HEDGE_NEUTRAL_SIGNAL);
    }
    score.filter(|v| v.is_finite()).ok_or_else(|| {
        format!(
            "return_score unavailable service={} note={}",
            service_name, note
        )
    })
}

fn resolve_inventory_hedge_signal_quantile(
    enable_return_score_adjust_hedge: bool,
    score: Option<f64>,
    score_quantile: Option<f64>,
    volatility: Option<f64>,
) -> Option<f64> {
    if !enable_return_score_adjust_hedge {
        return Some(INVENTORY_HEDGE_NEUTRAL_SIGNAL_QUANTILE);
    }
    if volatility.filter(|v| v.is_finite()).is_some() && score.filter(|v| v.is_finite()).is_none() {
        return Some(INVENTORY_HEDGE_NEUTRAL_SIGNAL_QUANTILE);
    }
    score_quantile.filter(|v| v.is_finite())
}

