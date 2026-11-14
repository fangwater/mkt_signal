pub use symbol_utils::symbol_util::*;

use anyhow::{bail, Result};

/// Build the canonical internal Hyperliquid perpetual symbol from its exact
/// wire coin and collateral token name.
///
/// HIP-3 wire coins include a DEX prefix such as `xyz:FOO`. Separators are not
/// part of the internal symbol, so that example with `USDH` collateral becomes
/// `XYZFOOUSDH`. Callers building a catalog must still reject collisions because
/// distinct wire strings can sanitize to the same internal symbol.
pub fn hyperliquid_internal_symbol(wire_coin: &str, collateral: &str) -> Result<String> {
    fn sanitize_component(value: &str, field: &str) -> Result<String> {
        if value.is_empty() {
            bail!("Hyperliquid {field} is empty");
        }
        if value.trim() != value {
            bail!("Hyperliquid {field} has surrounding whitespace: {value:?}");
        }
        if !value.is_ascii() {
            bail!("Hyperliquid {field} must be ASCII: {value:?}");
        }

        let sanitized: String = value
            .bytes()
            .filter(u8::is_ascii_alphanumeric)
            .map(|byte| byte.to_ascii_uppercase() as char)
            .collect();
        if sanitized.is_empty() {
            bail!("Hyperliquid {field} has no ASCII alphanumeric characters: {value:?}");
        }
        Ok(sanitized)
    }

    let wire_coin = sanitize_component(wire_coin, "wire coin")?;
    let collateral = sanitize_component(collateral, "collateral")?;
    Ok(format!("{wire_coin}{collateral}"))
}

#[cfg(test)]
mod hyperliquid_tests {
    use super::hyperliquid_internal_symbol;

    #[test]
    fn builds_default_and_hip3_internal_symbols() {
        assert_eq!(
            hyperliquid_internal_symbol("BTC", "USDC").unwrap(),
            "BTCUSDC"
        );
        assert_eq!(
            hyperliquid_internal_symbol("xyz:FOO", "USDH").unwrap(),
            "XYZFOOUSDH"
        );
    }

    #[test]
    fn rejects_ambiguous_empty_or_non_ascii_components() {
        assert!(hyperliquid_internal_symbol("", "USDC").is_err());
        assert!(hyperliquid_internal_symbol("BTC", "---").is_err());
        assert!(hyperliquid_internal_symbol(" BTC", "USDC").is_err());
        assert!(hyperliquid_internal_symbol("比特币", "USDC").is_err());
    }
}
