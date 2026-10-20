use account_common::BinanceAccountMode;
use order_common::TradingVenue;
use quote_plan::open_quote_plan::volatility_grid_offset;
use runtime_common::execution_backend::ExecBackend;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GridConfig {
    pub spot: GridSpotConfig,
    pub futures: GridFuturesConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GridSpotConfig {
    pub orders_per_batch: u32,
    pub maker_timeout_ms: u32,
    pub max_maker_requotes: u32,
}

impl Default for GridSpotConfig {
    fn default() -> Self {
        Self {
            orders_per_batch: 3,
            maker_timeout_ms: 1_000,
            max_maker_requotes: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GridFuturesConfig {
    pub orders_per_batch: u32,
    pub maker_timeout_ms: u32,
}

impl Default for GridFuturesConfig {
    fn default() -> Self {
        Self {
            orders_per_batch: 3,
            maker_timeout_ms: 1_000,
        }
    }
}

impl GridConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (market, count, timeout) in [
            (
                "spot",
                self.spot.orders_per_batch,
                self.spot.maker_timeout_ms,
            ),
            (
                "futures",
                self.futures.orders_per_batch,
                self.futures.maker_timeout_ms,
            ),
        ] {
            if count == 0 || timeout == 0 {
                return Err(format!(
                    "grid.{market} split count and maker duration must be positive"
                ));
            }
        }
        Ok(())
    }
}

// Algorithm internals, not user configuration.
pub const VOL_STALE_MS: u32 = 15_000;
pub const QUOTE_STALE_MS: u32 = 1_000;

pub fn grid_offset(level: u32, levels: u32, vol: f64) -> Result<f64, String> {
    if !vol.is_finite() || vol < 0.0 || levels == 0 || level >= levels {
        return Err("invalid grid volatility or level".into());
    }
    let offset = volatility_grid_offset(vol, levels as usize, level as usize, [0.0, 1.0], 0.0);
    if !offset.is_finite() || !(0.0..1.0).contains(&offset) {
        return Err("grid price offset must be in [0, 1)".into());
    }
    Ok(offset)
}

pub fn validate_grid_account(
    venue: TradingVenue,
    backend: ExecBackend,
    binance_mode: Option<BinanceAccountMode>,
) -> Result<(), String> {
    let allowed = match backend {
        ExecBackend::Ltp => matches!(
            venue,
            TradingVenue::BinanceMargin
                | TradingVenue::BinanceFutures
                | TradingVenue::OkexMargin
                | TradingVenue::OkexFutures
        ),
        ExecBackend::Native => match venue {
            TradingVenue::BinanceMargin
            | TradingVenue::BinanceFutures
            | TradingVenue::BinanceCoinFutures => binance_mode == Some(BinanceAccountMode::Unified),
            // Exec startup already verifies these native unified account modes.
            TradingVenue::OkexFutures
            | TradingVenue::BybitFutures
            | TradingVenue::BitgetFutures => true,
            _ => false,
        },
    };
    if allowed {
        Ok(())
    } else {
        Err("grid requires RapidX or a supported native unified account".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_account_gate_rejects_standard_and_unsupported_markets() {
        for venue in [
            TradingVenue::BinanceFutures,
            TradingVenue::BinanceCoinFutures,
            TradingVenue::BinanceMargin,
        ] {
            assert!(validate_grid_account(venue, ExecBackend::Native, None).is_err());
            assert!(validate_grid_account(
                venue,
                ExecBackend::Native,
                Some(BinanceAccountMode::Standard)
            )
            .is_err());
            assert!(validate_grid_account(
                venue,
                ExecBackend::Native,
                Some(BinanceAccountMode::Unified)
            )
            .is_ok());
        }
        for venue in [
            TradingVenue::BinanceFutures,
            TradingVenue::BinanceMargin,
            TradingVenue::OkexFutures,
            TradingVenue::OkexMargin,
        ] {
            assert!(validate_grid_account(venue, ExecBackend::Ltp, None).is_ok());
        }
        for venue in [
            TradingVenue::OkexFutures,
            TradingVenue::BybitFutures,
            TradingVenue::BitgetFutures,
        ] {
            assert!(validate_grid_account(venue, ExecBackend::Native, None).is_ok());
        }
        assert!(
            validate_grid_account(TradingVenue::GateFutures, ExecBackend::Native, None).is_err()
        );
        assert!(
            validate_grid_account(TradingVenue::BinanceCoinFutures, ExecBackend::Ltp, None)
                .is_err()
        );
    }

    #[test]
    fn fixed_grid_offsets_follow_volatility_without_user_controls() {
        for (level, expected) in [0.0, 0.005, 0.01].into_iter().enumerate() {
            assert!((grid_offset(level as u32, 3, 0.01).unwrap() - expected).abs() < 1e-12);
        }
        assert_eq!(grid_offset(0, 1, 0.01).unwrap(), 0.0);
        assert_eq!(grid_offset(0, 3, 0.0).unwrap(), 0.0);
        assert!(grid_offset(0, 3, f64::NAN).is_err());
        assert!(grid_offset(3, 3, 0.01).is_err());
        assert!(grid_offset(2, 3, 10.0).is_err());
        assert!(serde_json::from_value::<GridConfig>(
            serde_json::json!({"open_buy_vol_scale":[0,1]})
        )
        .is_err());
        assert!(serde_json::from_value::<GridConfig>(
            serde_json::json!({"futures":{"max_maker_requotes":2}})
        )
        .is_err());
    }
}
