use super::super::common::{build_decision_from_key_base, ReturnScoreThresholdsResolved};
use super::super::factor_value_hub::EnvironmentSignalResult;
use crate::pre_trade::order_manager::Side;

pub(crate) fn select_prediction_side(
    prediction_mode: bool,
    return_score: Option<f64>,
    thresholds: Option<ReturnScoreThresholdsResolved>,
) -> (Option<Side>, Option<f64>, bool, bool, bool) {
    let (Some(score), Some(thresholds)) = (return_score, thresholds) else {
        return (None, None, false, false, false);
    };

    let forward_open_hit = score > thresholds.forward_open;
    let backward_open_hit = score < thresholds.backward_open;
    let prediction_side = if prediction_mode {
        if forward_open_hit {
            Some(Side::Buy)
        } else if backward_open_hit {
            Some(Side::Sell)
        } else {
            None
        }
    } else {
        None
    };
    let prediction_threshold = prediction_side.map(|side| match side {
        Side::Buy => thresholds.forward_open,
        Side::Sell => thresholds.backward_open,
    });
    let prediction_ready = forward_open_hit || backward_open_hit;

    (
        prediction_side,
        prediction_threshold,
        forward_open_hit,
        backward_open_hit,
        prediction_ready,
    )
}

pub(crate) fn build_from_key(
    now_us: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env: &EnvironmentSignalResult,
) -> String {
    build_decision_from_key_base(
        now_us,
        return_score,
        return_threshold,
        volatility,
        env.score,
        env.threshold,
    )
}

fn format_tlen_value(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.8}")
    } else {
        "nan".to_string()
    }
}

pub(crate) fn format_batch_tlen_suffix(tick_tlens: &[(i64, f64)]) -> String {
    tick_tlens
        .iter()
        .map(|(tick_index, tlen)| format!("{tick_index}@{}", format_tlen_value(*tlen)))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn append_mm_open_tlens_to_from_key(
    base_from_key: &str,
    level_tick_index: i64,
    level_tlen: f64,
    batch_tick_tlens: &[(i64, f64)],
) -> String {
    format!(
        "{base_from_key}:px_tick={level_tick_index}:px_tlen={}:batch_tlen={}",
        format_tlen_value(level_tlen),
        format_batch_tlen_suffix(batch_tick_tlens)
    )
}

pub fn append_mm_hedge_tlens_to_from_key(
    base_from_key: &str,
    batch_tick_tlens: &[(i64, f64)],
) -> String {
    format!(
        "{base_from_key}:batch_tlen={}",
        format_batch_tlen_suffix(batch_tick_tlens)
    )
}

pub fn append_tlen_query_error_to_from_key(base_from_key: &str, err: &str) -> String {
    let sanitized = err
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ',') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{base_from_key}:tlen_query_err={sanitized}")
}
