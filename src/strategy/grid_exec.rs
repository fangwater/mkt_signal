use account_common::BinanceAccountMode;
use order_common::{Side, TradingVenue};
use quote_plan::open_quote_plan::volatility_grid_offset;
use runtime_common::execution_backend::ExecBackend;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GridConfig {
    pub open_buy_vol_scale: [f64; 2],
    pub open_sell_vol_scale: [f64; 2],
    pub open_offset_lower: f64,
    pub vol_stale_ms: u32,
    pub quote_stale_ms: u32,
}

impl Default for GridConfig {
    fn default() -> Self {
        Self {
            open_buy_vol_scale: [0.0, 1.0],
            open_sell_vol_scale: [0.0, 1.0],
            open_offset_lower: 0.0,
            vol_stale_ms: 15_000,
            quote_stale_ms: 1_000,
        }
    }
}

impl GridConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, range) in [
            ("open_buy_vol_scale", self.open_buy_vol_scale),
            ("open_sell_vol_scale", self.open_sell_vol_scale),
        ] {
            if !range.iter().all(|v| v.is_finite()) || range[0] < 0.0 || range[1] < range[0] {
                return Err(format!(
                    "grid.{name} must be a finite ordered non-negative range"
                ));
            }
        }
        if !self.open_offset_lower.is_finite() || self.open_offset_lower < 0.0 {
            return Err("grid.open_offset_lower must be finite and non-negative".into());
        }
        if self.vol_stale_ms == 0 || self.quote_stale_ms == 0 {
            return Err("grid freshness limits must be positive".into());
        }
        Ok(())
    }

    pub fn offset(&self, side: Side, level: u32, levels: u32, vol: f64) -> Result<f64, String> {
        if !vol.is_finite() || vol < 0.0 || levels == 0 || level >= levels {
            return Err("invalid grid volatility or level".into());
        }
        let range = match side {
            Side::Buy => self.open_buy_vol_scale,
            Side::Sell => self.open_sell_vol_scale,
        };
        let offset = volatility_grid_offset(
            vol,
            levels as usize,
            level as usize,
            range,
            self.open_offset_lower,
        );
        if !offset.is_finite() || offset < 0.0 || offset >= 1.0 {
            return Err("grid price offset must be in [0, 1)".into());
        }
        Ok(offset)
    }
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
    fn grid_ranges_clamp_the_inner_margin_and_preserve_outer_vol() {
        let mut config = GridConfig::default();
        config.open_buy_vol_scale = [0.1, 0.5];
        config.open_sell_vol_scale = [0.2, 0.8];
        config.open_offset_lower = 0.004;
        for (side, offsets) in [
            (Side::Buy, [0.004, 0.0045, 0.005]),
            (Side::Sell, [0.004, 0.006, 0.008]),
        ] {
            for (level, expected) in offsets.into_iter().enumerate() {
                assert!(
                    (config.offset(side, level as u32, 3, 0.01).unwrap() - expected).abs() < 1e-12
                );
            }
            assert_eq!(config.offset(side, 0, 1, 0.01).unwrap(), 0.004);
            assert_eq!(config.offset(side, 0, 3, 0.0).unwrap(), 0.0);
        }
        config.open_offset_lower = 0.1;
        assert_eq!(config.offset(Side::Buy, 0, 3, 0.01).unwrap(), 0.005);
        assert!(config.offset(Side::Buy, 0, 3, f64::NAN).is_err());
        assert!(config.offset(Side::Buy, 3, 3, 0.01).is_err());
        assert!(config.offset(Side::Buy, 0, 3, 10.0).is_err());
        assert!(config.offset(Side::Buy, 2, 3, 10.0).is_err());
        config.open_buy_vol_scale = [1.0, 0.0];
        assert!(config.validate().is_err());
    }
}
