use std::sync::OnceLock;

pub const BITGET_PUBLIC_API_BASE_ENV: &str = "BITGET_PUBLIC_API_BASE";
pub const DEFAULT_BITGET_PUBLIC_API_BASE: &str = "https://api.bitget.com";

pub fn bitget_public_api_base() -> &'static str {
    static BASE: OnceLock<String> = OnceLock::new();
    BASE.get_or_init(|| normalized_base(std::env::var(BITGET_PUBLIC_API_BASE_ENV).ok().as_deref()))
}

pub fn bitget_public_api_url(path_and_query: &str) -> String {
    join_base_and_path(bitget_public_api_base(), path_and_query)
}

fn normalized_base(configured: Option<&str>) -> String {
    configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_BITGET_PUBLIC_API_BASE)
        .trim_end_matches('/')
        .to_string()
}

fn join_base_and_path(base: &str, path_and_query: &str) -> String {
    let suffix = path_and_query.trim();
    if suffix.starts_with('/') {
        format!("{}{}", base.trim_end_matches('/'), suffix)
    } else {
        format!("{}/{}", base.trim_end_matches('/'), suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_configured_base() {
        assert_eq!(
            normalized_base(Some(" http://127.0.0.1:28902/// ")),
            "http://127.0.0.1:28902"
        );
        assert_eq!(normalized_base(Some("  ")), DEFAULT_BITGET_PUBLIC_API_BASE);
        assert_eq!(normalized_base(None), DEFAULT_BITGET_PUBLIC_API_BASE);
    }

    #[test]
    fn joins_path_without_duplicate_slashes() {
        assert_eq!(
            join_base_and_path("http://127.0.0.1:28902/", "/api/v3/market/instruments"),
            "http://127.0.0.1:28902/api/v3/market/instruments"
        );
        assert_eq!(
            join_base_and_path("https://api.bitget.com", "api/v3/market/instruments"),
            "https://api.bitget.com/api/v3/market/instruments"
        );
    }
}
