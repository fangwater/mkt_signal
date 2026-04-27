use super::super::common::{
    append_tlen_to_from_key, build_decision_from_key_base, format_tlen_value,
};
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
    tlen: Option<f64>,
    tlen_threshold: Option<f64>,
) -> String {
    let mut key = build_from_key(now_us, return_qtl, cancel_threshold, volatility, env);
    key.push_str(":tlen=");
    key.push_str(&format_tlen_value(tlen.unwrap_or(0.0)));
    key.push_str(":tlen_thr=");
    key.push_str(&format_tlen_value(tlen_threshold.unwrap_or(0.0)));
    key
}

pub fn append_mm_open_tlens_to_from_key(base_from_key: &str, level_tlen: f64) -> String {
    append_tlen_to_from_key(base_from_key, level_tlen)
}

pub fn append_mm_hedge_tlen_to_from_key(base_from_key: &str, level_tlen: f64) -> String {
    append_tlen_to_from_key(base_from_key, level_tlen)
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
        let key = append_mm_open_tlens_to_from_key("123:ret_qtl=1", 4.25);
        assert_eq!(key, "123:ret_qtl=1:tlen=4.25000000");
    }

    #[test]
    fn mm_open_from_key_uses_zero_tlen_fallback() {
        let key = append_mm_open_tlens_to_from_key("123:ret_qtl=1", 0.0);
        assert_eq!(key, "123:ret_qtl=1:tlen=0.00000000");
    }

    #[test]
    fn mm_hedge_from_key_appends_only_single_tlen() {
        let key = append_mm_hedge_tlen_to_from_key("123:ret_qtl=1", 2.5);
        assert_eq!(key, "123:ret_qtl=1:tlen=2.50000000");
    }

    #[test]
    fn mm_hedge_from_key_uses_zero_tlen_fallback() {
        let key = append_mm_hedge_tlen_to_from_key("123:ret_qtl=1", 0.0);
        assert_eq!(key, "123:ret_qtl=1:tlen=0.00000000");
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
            score_quantile: None,
            threshold: Some(0.4),
            note: String::new(),
        };
        let key = build_mm_cancel_from_key(123, None, None, None, &env, Some(1.25), Some(3.5));
        assert_eq!(
            key,
            "123:ret_qtl=0:ret_thr=0:vol=0:env_score=0.30000000:env_thr=0.40000000:tlen=1.25000000:tlen_thr=3.50000000"
        );
    }

    #[test]
    fn mm_cancel_from_key_always_contains_tlen_fields() {
        let env = EnvironmentSignalResult {
            source: EnvironmentSignalSource::PnluFallback,
            allow_open: true,
            class_label: 0,
            service_name: None,
            symbol_key: "BTCUSDT".to_string(),
            score: None,
            score_quantile: None,
            threshold: None,
            note: String::new(),
        };
        let key = build_mm_cancel_from_key(123, Some(0.5), Some(0.2), Some(0.1), &env, None, None);
        assert_eq!(
            key,
            "123:ret_qtl=0.50000000:ret_thr=0.20000000:vol=0.10000000:env_score=0:env_thr=0:tlen=0.00000000:tlen_thr=0.00000000"
        );
    }
}
