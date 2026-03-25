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

pub(crate) fn build_mm_cancel_from_key(
    now_us: i64,
    return_score: Option<f64>,
    cancel_threshold: Option<f64>,
    volatility: Option<f64>,
    env: &EnvironmentSignalResult,
    tlen: Option<f64>,
    tlen_threshold: Option<f64>,
) -> String {
    let mut key = build_from_key(
        now_us,
        return_score,
        cancel_threshold,
        volatility,
        env,
    );
    if let Some(value) = tlen {
        key.push_str(":tlen=");
        key.push_str(&format_tlen_value(value));
    }
    if let Some(value) = tlen_threshold {
        key.push_str(":tlen_thr=");
        key.push_str(&format_tlen_value(value));
    }
    key
}

pub fn append_mm_open_tlens_to_from_key(base_from_key: &str, level_tlen: f64) -> String {
    format!("{base_from_key}:tlen={}", format_tlen_value(level_tlen))
}

pub fn append_mm_hedge_tlen_to_from_key(base_from_key: &str, level_tlen: f64) -> String {
    format!("{base_from_key}:tlen={}", format_tlen_value(level_tlen))
}

#[cfg(test)]
mod tests {
    use super::{
        append_mm_hedge_tlen_to_from_key, append_mm_open_tlens_to_from_key,
        build_mm_cancel_from_key,
    };
    use crate::funding_rate::factor_value_hub::{EnvironmentSignalResult, EnvironmentSignalSource};

    #[test]
    fn mm_open_from_key_appends_only_single_tlen() {
        let key = append_mm_open_tlens_to_from_key("123:ret_score=1", 4.25);
        assert_eq!(key, "123:ret_score=1:tlen=4.25000000");
    }

    #[test]
    fn mm_open_from_key_uses_zero_tlen_fallback() {
        let key = append_mm_open_tlens_to_from_key("123:ret_score=1", 0.0);
        assert_eq!(key, "123:ret_score=1:tlen=0.00000000");
    }

    #[test]
    fn mm_hedge_from_key_appends_only_single_tlen() {
        let key = append_mm_hedge_tlen_to_from_key("123:ret_score=1", 2.5);
        assert_eq!(key, "123:ret_score=1:tlen=2.50000000");
    }

    #[test]
    fn mm_hedge_from_key_uses_zero_tlen_fallback() {
        let key = append_mm_hedge_tlen_to_from_key("123:ret_score=1", 0.0);
        assert_eq!(key, "123:ret_score=1:tlen=0.00000000");
    }

    #[test]
    fn mm_cancel_from_key_uses_unified_shape() {
        let env = EnvironmentSignalResult {
            source: EnvironmentSignalSource::PnluFallback,
            allow_open: true,
            class_label: 0,
            service_name: Some("env".to_string()),
            symbol_key: "BTCUSDT".to_string(),
            score: Some(0.3),
            threshold: Some(0.4),
            note: String::new(),
        };
        let key = build_mm_cancel_from_key(
            123,
            None,
            None,
            None,
            &env,
            Some(1.25),
            Some(3.5),
        );
        assert_eq!(
            key,
            "123:ret_score=0:ret_thr=0:vol=0:env_score=0.30000000:env_thr=0.40000000:tlen=1.25000000:tlen_thr=3.50000000"
        );
    }
}
