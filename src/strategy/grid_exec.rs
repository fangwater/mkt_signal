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
    pub vol_multiplier: f64,
    /// Additional maker rounds after the first; None means unlimited.
    pub max_maker_requotes: Option<u32>,
}

impl Default for GridSpotConfig {
    fn default() -> Self {
        Self {
            orders_per_batch: 3,
            maker_timeout_ms: 1_000,
            vol_multiplier: 1.0,
            max_maker_requotes: Some(2),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GridFuturesConfig {
    pub orders_per_batch: u32,
    pub maker_timeout_ms: u32,
    pub vol_multiplier: f64,
    pub max_maker_requotes: Option<u32>,
}

impl Default for GridFuturesConfig {
    fn default() -> Self {
        Self {
            orders_per_batch: 3,
            maker_timeout_ms: 1_000,
            vol_multiplier: 1.0,
            max_maker_requotes: None,
        }
    }
}

impl GridConfig {
    pub fn vol_multiplier(&self, cash: bool) -> f64 {
        if cash {
            self.spot.vol_multiplier
        } else {
            self.futures.vol_multiplier
        }
    }

    pub fn max_maker_requotes(&self, cash: bool) -> Option<u32> {
        if cash {
            self.spot.max_maker_requotes
        } else {
            self.futures.max_maker_requotes
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        for (market, count, timeout, multiplier, requotes) in [
            (
                "spot",
                self.spot.orders_per_batch,
                self.spot.maker_timeout_ms,
                self.spot.vol_multiplier,
                self.spot.max_maker_requotes,
            ),
            (
                "futures",
                self.futures.orders_per_batch,
                self.futures.maker_timeout_ms,
                self.futures.vol_multiplier,
                self.futures.max_maker_requotes,
            ),
        ] {
            if count == 0 || timeout == 0 {
                return Err(format!(
                    "grid.{market} split count and maker duration must be positive"
                ));
            }
            if !multiplier.is_finite() || multiplier <= 0.0 {
                return Err(format!(
                    "grid.{market}.vol_multiplier must be finite and positive"
                ));
            }
            if requotes == Some(u32::MAX) {
                return Err(format!("grid.{market}.max_maker_requotes must be less than uint32 max; use null for unlimited"));
            }
        }
        Ok(())
    }
}

// Algorithm internals, not user configuration.
pub const VOL_STALE_MS: u32 = 15_000;
pub const QUOTE_STALE_MS: u32 = 1_000;

pub fn grid_offset(level: u32, levels: u32, vol: f64, multiplier: f64) -> Result<f64, String> {
    if !vol.is_finite()
        || vol < 0.0
        || !multiplier.is_finite()
        || multiplier <= 0.0
        || levels == 0
        || level >= levels
    {
        return Err("invalid grid volatility or level".into());
    }
    let offset =
        volatility_grid_offset(vol, levels as usize, level as usize, [0.0, multiplier], 0.0);
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
            // USD-M uses the existing STANDARD order, cancel and account paths.
            // Enabling Grid does not expand this Exec into cash or COIN-M.
            TradingVenue::BinanceFutures => matches!(
                binance_mode,
                Some(BinanceAccountMode::Standard | BinanceAccountMode::Unified)
            ),
            TradingVenue::BinanceMargin | TradingVenue::BinanceCoinFutures => {
                binance_mode == Some(BinanceAccountMode::Unified)
            }
            // Exec startup already verifies these native unified account modes.
            TradingVenue::OkexMargin
            | TradingVenue::OkexFutures
            | TradingVenue::BybitFutures
            | TradingVenue::BitgetFutures => true,
            _ => false,
        },
    };
    if allowed {
        Ok(())
    } else {
        Err(
            "grid requires RapidX, native Binance USD-M, or a supported native unified account"
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_account_gate_allows_standard_usdm_only() {
        for venue in [
            TradingVenue::BinanceFutures,
            TradingVenue::BinanceCoinFutures,
            TradingVenue::BinanceMargin,
        ] {
            assert!(validate_grid_account(venue, ExecBackend::Native, None).is_err());
            assert_eq!(
                validate_grid_account(
                    venue,
                    ExecBackend::Native,
                    Some(BinanceAccountMode::Standard)
                )
                .is_ok(),
                venue == TradingVenue::BinanceFutures
            );
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
            TradingVenue::OkexMargin,
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
    fn grid_offsets_scale_volatility_and_validate_controls() {
        for (level, expected) in [0.0, 0.005, 0.01].into_iter().enumerate() {
            assert!((grid_offset(level as u32, 3, 0.01, 1.0).unwrap() - expected).abs() < 1e-12);
        }
        assert_eq!(grid_offset(0, 1, 0.01, 1.0).unwrap(), 0.0);
        assert_eq!(grid_offset(0, 3, 0.0, 1.0).unwrap(), 0.0);
        assert!(grid_offset(0, 3, f64::NAN, 1.0).is_err());
        assert!(grid_offset(3, 3, 0.01, 1.0).is_err());
        assert!(grid_offset(2, 3, 10.0, 1.0).is_err());
        assert!(serde_json::from_value::<GridConfig>(
            serde_json::json!({"open_buy_vol_scale":[0,1]})
        )
        .is_err());
        assert_eq!(grid_offset(2, 3, 0.01, 2.0).unwrap(), 0.02);
        assert_eq!(grid_offset(1, 3, 0.01, 0.5).unwrap(), 0.0025);
        assert_eq!(grid_offset(0, 1, 0.01, 2.0).unwrap(), 0.0);
        assert!(grid_offset(2, 3, 0.5, 2.0).is_err());
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(grid_offset(0, 3, 0.01, invalid).is_err());
            for cash in [true, false] {
                let mut config = GridConfig::default();
                if cash {
                    config.spot.vol_multiplier = invalid;
                } else {
                    config.futures.vol_multiplier = invalid;
                }
                assert!(config.validate().is_err());
            }
        }
        let config: GridConfig = serde_json::from_value(serde_json::json!({
            "spot": {"max_maker_requotes": null, "vol_multiplier": 0.5},
            "futures": {"max_maker_requotes": 0, "vol_multiplier": 2.0}
        }))
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.max_maker_requotes(true), None);
        assert_eq!(config.max_maker_requotes(false), Some(0));
        assert_eq!(config.vol_multiplier(true), 0.5);
        assert_eq!(config.vol_multiplier(false), 2.0);
        let defaults: GridConfig = serde_json::from_str(r#"{"spot":{},"futures":{}}"#).unwrap();
        assert_eq!(defaults.spot.max_maker_requotes, Some(2));
        assert_eq!(defaults.futures.max_maker_requotes, None);
    }
}
