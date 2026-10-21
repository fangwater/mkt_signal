use super::interval::BarInterval;

/// Publisher and subscribers must use the same optional instance scope.
/// BBO services stay shared so each instance can size against live market data.
pub fn model_service_path(venue: &str, factor: &str) -> String {
    model_service_path_for_interval(venue, factor, BarInterval::OneMinute)
}

pub fn model_service_path_for_interval(venue: &str, factor: &str, interval: BarInterval) -> String {
    scoped_model_service_path(
        venue,
        factor,
        interval,
        std::env::var("CTA_PERP_MODEL_SCOPE").ok().as_deref(),
    )
}

fn scoped_model_service_path(
    venue: &str,
    factor: &str,
    interval: BarInterval,
    scope: Option<&str>,
) -> String {
    let scope = scope.unwrap_or_default().trim();
    let prefix = if scope.is_empty() {
        "model_output/".to_string()
    } else {
        format!("model_output/{scope}/")
    };
    format!(
        "{prefix}cta_perp-{venue}-{}-{}",
        interval.as_str(),
        factor.to_ascii_lowercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_minute_channels_cannot_collide_with_one_minute_channels() {
        for scope in [None, Some("rx04")] {
            let minute = scoped_model_service_path(
                "binance-futures",
                "avg_price",
                BarInterval::OneMinute,
                scope,
            );
            let five = scoped_model_service_path(
                "binance-futures",
                "avg_price",
                BarInterval::FiveMinutes,
                scope,
            );
            assert_ne!(minute, five);
            assert!(five.ends_with("cta_perp-binance-futures-5m-avg_price"));
        }
    }

    #[test]
    fn independent_instances_do_not_share_factor_services() {
        for factor in ["baseline_164", "baseline_200", "TD_MT_029"] {
            let shared =
                scoped_model_service_path("binance-futures", factor, BarInterval::OneMinute, None);
            let rx02 = scoped_model_service_path(
                "binance-futures",
                factor,
                BarInterval::OneMinute,
                Some("rx02"),
            );
            let rx03 = scoped_model_service_path(
                "binance-futures",
                factor,
                BarInterval::OneMinute,
                Some("rx03"),
            );
            assert_ne!(shared, rx03);
            assert_ne!(rx02, rx03);
            assert_eq!(
                shared,
                format!(
                    "model_output/cta_perp-binance-futures-1m-{}",
                    factor.to_ascii_lowercase()
                )
            );
            assert_eq!(
                rx03,
                format!(
                    "model_output/rx03/cta_perp-binance-futures-1m-{}",
                    factor.to_ascii_lowercase()
                )
            );
        }
    }
}
