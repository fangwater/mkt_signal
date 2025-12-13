/// 从 symbol / inst_id 中提取基础资产（如 BTCUSDT -> BTC，BTC-USDT-SWAP -> BTC）
pub fn extract_base_asset(symbol_like: &str) -> Option<String> {
    let upper = symbol_like.to_uppercase();

    // OKX style: BTC-USDT-SWAP / BTC-USDT / BTC-USD-SWAP
    if upper.contains('-') {
        return upper.split('-').next().map(|s| s.to_string());
    }

    const QUOTES: [&str; 7] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY", "USD"];
    for quote in QUOTES {
        if upper.ends_with(quote) && upper.len() > quote.len() {
            return Some(upper[..upper.len() - quote.len()].to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::extract_base_asset;

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
    }

    #[test]
    fn extract_base_asset_returns_none_for_unknown() {
        assert_eq!(extract_base_asset("BTC").as_deref(), None);
        assert_eq!(extract_base_asset("FOOXYZ").as_deref(), None);
    }
}
