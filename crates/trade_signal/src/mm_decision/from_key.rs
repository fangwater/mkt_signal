use super::super::common::{append_tlen_threshold, build_decision_from_key_base};
use super::super::factor_value_hub::EnvironmentSignalResult;

pub(crate) fn build_from_key(
    now_us: i64,
    return_qtl: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env: &EnvironmentSignalResult,
) -> String {
    build_decision_from_key_base(
        now_us,
        return_qtl,
        return_threshold,
        volatility,
        env.score,
        env.threshold,
    )
}

pub(crate) fn build_mm_cancel_from_key(
    now_us: i64,
    return_qtl: Option<f64>,
    cancel_threshold: Option<f64>,
    volatility: Option<f64>,
    env: &EnvironmentSignalResult,
    tlen_threshold: Option<f64>,
) -> String {
    append_tlen_threshold(
        build_from_key(now_us, return_qtl, cancel_threshold, volatility, env),
        tlen_threshold,
    )
}

#[cfg(test)]
mod tests {
    use super::build_mm_cancel_from_key;
    use crate::factor_value_hub::{EnvironmentSignalResult, EnvironmentSignalSource};

    #[test]
    fn mm_cancel_from_key_keeps_threshold_without_tlen() {
        let env = EnvironmentSignalResult {
            source: EnvironmentSignalSource::PnluFallback,
            allow_open: true,
            class_label: 0,
            service_name: Some("env".to_string()),
            symbol_key: "BTCUSDT".to_string(),
            score: Some(0.3),
            score_quantile: None,
            threshold: Some(0.4),
            note: String::new(),
        };
        let key = build_mm_cancel_from_key(123, None, None, None, &env, Some(3.5));
        assert_eq!(
            key,
            "123:ret_qtl=0:ret_thr=0:vol=0:env_score=0.30000000:env_thr=0.40000000:tlen_thr=3.50000000"
        );
        assert!(!key.contains(":tlen="));
    }
}
