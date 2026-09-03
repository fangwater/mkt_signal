/// Assets treated as non-directional cash/collateral by strategy exposure risk.
///
/// Their balances and liabilities remain in account/equity accounting; this helper only
/// controls position exposure, concentration, and automatic close-symbol selection.
pub fn is_exposure_exempt_asset(asset: &str) -> bool {
    let asset = asset.trim();
    asset.eq_ignore_ascii_case("USDT")
        || asset.eq_ignore_ascii_case("USDC")
        || asset.eq_ignore_ascii_case("BFUSD")
}

/// 从 symbol / inst_id 中提取基础资产（如 BTCUSDT -> BTC，BTC-USDT-SWAP -> BTC）
pub fn extract_base_asset(symbol_like: &str) -> Option<String> {
    let upper = symbol_like.to_uppercase();
    let upper = if let Some(root) = upper.strip_suffix("_CM") {
        root.to_string()
    } else if let Some(root) = upper.strip_suffix("CM") {
        root.trim_end_matches('_').to_string()
    } else if let Some(root) = upper.strip_suffix("_PERP") {
        root.to_string()
    } else if let Some(root) = upper.strip_suffix("PERP") {
        root.trim_end_matches('_').to_string()
    } else if upper.len() > 7
        && upper
            .get(upper.len() - 6..)
            .is_some_and(|suffix| suffix.bytes().all(|byte| byte.is_ascii_digit()))
    {
        upper[..upper.len() - 6].trim_end_matches('_').to_string()
    } else {
        upper
    };
    const QUOTES: [&str; 7] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY", "USD"];

    // OKX style: BTC-USDT-SWAP / BTC-USDT / BTC-USD-SWAP
    if upper.contains('-') {
        return upper.split('-').next().map(|s| s.to_string());
    }

    // Gate style: BTC_USDT / ETH_USDC
    for quote in QUOTES {
        let suffix = format!("_{quote}");
        if upper.ends_with(&suffix) && upper.len() > suffix.len() {
            return Some(upper[..upper.len() - suffix.len()].to_string());
        }
    }

    for quote in QUOTES {
        if upper.ends_with(quote) && upper.len() > quote.len() {
            return Some(upper[..upper.len() - quote.len()].to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{extract_base_asset, is_exposure_exempt_asset};

    #[test]
    fn exposure_exempt_assets_are_stable_collateral_assets() {
        for asset in ["USDT", "usdc", " BFUSD "] {
            assert!(is_exposure_exempt_asset(asset), "asset={asset}");
        }
        for asset in ["BUSD", "FDUSD", "BTC", "FUSD"] {
            assert!(!is_exposure_exempt_asset(asset), "asset={asset}");
        }
    }

    #[test]
    fn extract_base_asset_handles_okx_symbols() {
        assert_eq!(extract_base_asset("BTC-USDT-SWAP").as_deref(), Some("BTC"));
        assert_eq!(extract_base_asset("BTC-USD-SWAP").as_deref(), Some("BTC"));
        assert_eq!(extract_base_asset("ETH-USDT").as_deref(), Some("ETH"));
    }

    #[test]
    fn extract_base_asset_handles_concat_quotes() {
        assert_eq!(extract_base_asset("BTCUSDT").as_deref(), Some("BTC"));
        assert_eq!(extract_base_asset("BTCUSD").as_deref(), Some("BTC"));
        assert_eq!(extract_base_asset("ethusdc").as_deref(), Some("ETH"));
        assert_eq!(extract_base_asset("BTCUSD_PERP").as_deref(), Some("BTC"));
        assert_eq!(extract_base_asset("ETHUSD_260925").as_deref(), Some("ETH"));
        assert_eq!(extract_base_asset("BTCUSD_CM").as_deref(), Some("BTC"));
        assert_eq!(extract_base_asset("BTCUSDCM").as_deref(), Some("BTC"));
    }

    #[test]
    fn extract_base_asset_handles_gate_underscore_symbols() {
        assert_eq!(extract_base_asset("SOL_USDT").as_deref(), Some("SOL"));
        assert_eq!(extract_base_asset("eth_usdc").as_deref(), Some("ETH"));
    }

    #[test]
    fn extract_base_asset_returns_none_for_unknown() {
        assert_eq!(extract_base_asset("BTC").as_deref(), None);
        assert_eq!(extract_base_asset("FOOXYZ").as_deref(), None);
    }
}
