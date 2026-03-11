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
