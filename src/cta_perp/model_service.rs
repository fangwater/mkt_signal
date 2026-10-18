/// Publisher and subscribers must use the same optional instance scope.
/// BBO services stay shared so each instance can size against live market data.
pub fn model_service_path(venue: &str, factor: &str) -> String {
    scoped_model_service_path(
        venue,
        factor,
        std::env::var("CTA_PERP_MODEL_SCOPE").ok().as_deref(),
    )
}

fn scoped_model_service_path(venue: &str, factor: &str, scope: Option<&str>) -> String {
    let scope = scope.unwrap_or_default().trim();
    let prefix = if scope.is_empty() {
        "model_output/".to_string()
    } else {
        format!("model_output/{scope}/")
    };
    format!(
        "{prefix}cta_perp-{venue}-1m-{}",
        factor.to_ascii_lowercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_instances_do_not_share_factor_services() {
        for factor in ["baseline_164", "baseline_200", "TD_MT_029"] {
            let shared = scoped_model_service_path("binance-futures", factor, None);
            let rx02 = scoped_model_service_path("binance-futures", factor, Some("rx02"));
            let rx03 = scoped_model_service_path("binance-futures", factor, Some("rx03"));
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
