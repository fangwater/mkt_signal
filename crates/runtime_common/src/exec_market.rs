//! A pure-execution deployment owns one market, even with shared account collateral.
use anyhow::{bail, ensure, Result};
use order_common::TradingVenue;

pub fn parse_venue(value: &str) -> Result<TradingVenue> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "binance-futures" => Ok(TradingVenue::BinanceFutures),
        "binance-coin-futures" => Ok(TradingVenue::BinanceCoinFutures),
        "okex-futures" => Ok(TradingVenue::OkexFutures),
        _ => bail!("unsupported Exec market: {value}"),
    }
}

pub fn resolve_venue(exec: Option<&str>, start: Option<&str>) -> Result<Option<TradingVenue>> {
    let exec = exec
        .filter(|value| !value.trim().is_empty())
        .map(parse_venue)
        .transpose()?;
    let start = start
        .filter(|value| !value.trim().is_empty())
        .map(parse_venue)
        .transpose()?;
    ensure!(
        exec.is_none() || start.is_none() || exec == start,
        "EXEC_VENUE and EXEC_START_VENUE must name the same market"
    );
    Ok(exec.or(start))
}

pub fn configured_venue() -> Result<Option<TradingVenue>> {
    resolve_venue(
        std::env::var("EXEC_VENUE").ok().as_deref(),
        std::env::var("EXEC_START_VENUE").ok().as_deref(),
    )
}

pub fn validate_symbol(venue: TradingVenue, symbol: &str) -> Result<()> {
    let normalized = crate::symbol_util::normalize_symbol_for_internal(symbol);
    let valid = match venue {
        TradingVenue::BinanceFutures => {
            normalized.ends_with("USDT") || normalized.ends_with("USDC")
        }
        TradingVenue::BinanceCoinFutures => normalized.len() > 3 && normalized.ends_with("USD"),
        TradingVenue::OkexFutures => true,
        _ => false,
    };
    ensure!(
        valid,
        "symbol {symbol} does not belong to Exec market {}",
        venue.data_pub_slug()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflicting_exec_markets_fail_before_startup() {
        assert!(resolve_venue(Some("binance-futures"), Some("binance-coin-futures")).is_err());
        assert_eq!(
            resolve_venue(None, Some("binance-coin-futures")).unwrap(),
            Some(TradingVenue::BinanceCoinFutures)
        );
        assert_eq!(resolve_venue(None, None).unwrap(), None);
        assert!(resolve_venue(Some("binance-margin"), None).is_err());
    }

    #[test]
    fn opposite_market_targets_are_rejected() {
        for symbol in ["BTCUSDT", "BTCUSDC"] {
            assert!(validate_symbol(TradingVenue::BinanceFutures, symbol).is_ok());
            assert!(validate_symbol(TradingVenue::BinanceCoinFutures, symbol).is_err());
        }
        for symbol in ["BTCUSD", "BTCUSD_PERP"] {
            assert!(validate_symbol(TradingVenue::BinanceCoinFutures, symbol).is_ok());
            assert!(validate_symbol(TradingVenue::BinanceFutures, symbol).is_err());
        }
    }
}
