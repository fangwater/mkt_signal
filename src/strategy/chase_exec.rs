use crate::strategy::batch_exec_strategy::{
    validate_target_signal, BatchExecTarget, MakerPriceAnchor,
};
use order_common::TradingVenue;
use serde::{Deserialize, Serialize};

pub const CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME: &str = "SYSTEM_POSITION_CLOSE";

const fn default_bbo_max_age_ms() -> u32 {
    2_000
}

/// ChaseExec configuration: a single level-0 post-only quote that follows the
/// opposite-side anchor via in-place amend, with fill-driven (water-level)
/// release instead of batch scheduling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChaseExecConfig {
    /// Maximum notional released per child order.
    pub single_order_usdt: f64,
    /// Maximum unfilled maker exposure (usdt) open at any time.
    pub max_open_usdt: f64,
    pub maker_price_anchor: MakerPriceAnchor,
    /// Anchor movement (bps of opposite best) required before a live child is
    /// amended. 0 amends whenever the aligned price actually changes.
    pub maker_recenter_trigger_bps: f64,
    /// Per-child minimum delay between amend requests.
    #[serde(default)]
    pub maker_amend_cooldown_ms: u32,
    /// Maker child lifetime; on expiry the remainder escalates to taker.
    pub maker_timeout_ms: u32,
    pub target_tolerance_usdt: f64,
    #[serde(default = "default_bbo_max_age_ms")]
    pub bbo_max_age_ms: u32,
}

impl Default for ChaseExecConfig {
    fn default() -> Self {
        Self {
            single_order_usdt: 100.0,
            max_open_usdt: 200.0,
            maker_price_anchor: MakerPriceAnchor::OppositeBestPlusOneTick,
            maker_recenter_trigger_bps: 3.0,
            maker_amend_cooldown_ms: 0,
            maker_timeout_ms: 60_000,
            target_tolerance_usdt: 10.0,
            bbo_max_age_ms: default_bbo_max_age_ms(),
        }
    }
}

impl ChaseExecConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.single_order_usdt.is_finite() || self.single_order_usdt <= 0.0 {
            return Err("single_order_usdt must be positive".to_string());
        }
        if !self.max_open_usdt.is_finite() || self.max_open_usdt <= 0.0 {
            return Err("max_open_usdt must be positive".to_string());
        }
        if !self.maker_recenter_trigger_bps.is_finite() || self.maker_recenter_trigger_bps < 0.0 {
            return Err("maker_recenter_trigger_bps must be finite and non-negative".to_string());
        }
        if self.maker_timeout_ms == 0 {
            return Err("maker_timeout_ms must be positive".to_string());
        }
        if !self.target_tolerance_usdt.is_finite() || self.target_tolerance_usdt < 0.0 {
            return Err("target_tolerance_usdt must be finite and non-negative".to_string());
        }
        if self.bbo_max_age_ms == 0 {
            return Err("bbo_max_age_ms must be positive".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChaseExecConfigOverride {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_order_usdt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_open_usdt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_price_anchor: Option<MakerPriceAnchor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_recenter_trigger_bps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_amend_cooldown_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_timeout_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_tolerance_usdt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbo_max_age_ms: Option<u32>,
}

impl ChaseExecConfigOverride {
    pub fn is_empty(&self) -> bool {
        self.single_order_usdt.is_none()
            && self.max_open_usdt.is_none()
            && self.maker_price_anchor.is_none()
            && self.maker_recenter_trigger_bps.is_none()
            && self.maker_amend_cooldown_ms.is_none()
            && self.maker_timeout_ms.is_none()
            && self.target_tolerance_usdt.is_none()
            && self.bbo_max_age_ms.is_none()
    }

    pub fn apply_to(&self, defaults: &ChaseExecConfig) -> ChaseExecConfig {
        ChaseExecConfig {
            single_order_usdt: self.single_order_usdt.unwrap_or(defaults.single_order_usdt),
            max_open_usdt: self.max_open_usdt.unwrap_or(defaults.max_open_usdt),
            maker_price_anchor: self
                .maker_price_anchor
                .unwrap_or(defaults.maker_price_anchor),
            maker_recenter_trigger_bps: self
                .maker_recenter_trigger_bps
                .unwrap_or(defaults.maker_recenter_trigger_bps),
            maker_amend_cooldown_ms: self
                .maker_amend_cooldown_ms
                .unwrap_or(defaults.maker_amend_cooldown_ms),
            maker_timeout_ms: self.maker_timeout_ms.unwrap_or(defaults.maker_timeout_ms),
            target_tolerance_usdt: self
                .target_tolerance_usdt
                .unwrap_or(defaults.target_tolerance_usdt),
            bbo_max_age_ms: self.bbo_max_age_ms.unwrap_or(defaults.bbo_max_age_ms),
        }
    }

    pub fn validate(&self, defaults: &ChaseExecConfig) -> Result<(), String> {
        if self.is_empty() {
            return Err("symbol override must replace at least one parameter".to_string());
        }
        self.apply_to(defaults).validate()
    }
}

/// Re-export so the Redis `targets` schema stays identical to batch_exec:
/// either a bare qty or `{"qty": .., "signal": ..}`.
pub type ChaseExecTarget = BatchExecTarget;
pub use crate::strategy::batch_exec_strategy::ALLOWED_TARGET_SIGNALS as CHASE_EXEC_ALLOWED_TARGET_SIGNALS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChaseExecCompletionReason {
    TargetReached,
    TargetTolerance,
    ExchangeMinimum,
    SymbolNotTradable,
}

impl ChaseExecCompletionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TargetReached => "target_reached",
            Self::TargetTolerance => "target_tolerance",
            Self::ExchangeMinimum => "exchange_minimum",
            Self::SymbolNotTradable => "symbol_not_tradable",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChaseExecSnapshot {
    pub algorithm: String,
    pub pov: Option<viz_common::resample::ExecPovState>,
    pub strategy_name: String,
    pub source_updated_at_ms: i64,
    pub symbol: String,
    pub exec_venue: TradingVenue,
    /// Physical net position shared by every exec strategy on this symbol.
    pub account_position_qty: f64,
    /// Position allocated to this strategy in the internal ledger.
    pub position_qty: f64,
    pub effective_position_qty: f64,
    pub position_allocated: bool,
    pub target_qty: Option<f64>,
    /// Unfilled remainder escalated to taker (base qty, signed).
    pub pending_qty: f64,
    /// Unfilled open order qty across live children (signed).
    pub live_order_qty: f64,
    pub live_children: usize,
    pub execution_complete: bool,
    pub completion_reason: String,
}

pub fn validate_chase_target(target: &ChaseExecTarget) -> Result<(), String> {
    if !target.qty.is_finite() {
        return Err("qty must be finite".to_string());
    }
    validate_target_signal(target.signal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        ChaseExecConfig::default().validate().unwrap();
    }

    #[test]
    fn config_rejects_bad_values() {
        let mut cfg = ChaseExecConfig::default();
        cfg.single_order_usdt = 0.0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.max_open_usdt = -1.0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.maker_recenter_trigger_bps = -0.5;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.maker_timeout_ms = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.target_tolerance_usdt = -1.0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.bbo_max_age_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn override_applies_and_validates() {
        let defaults = ChaseExecConfig::default();
        let override_cfg: ChaseExecConfigOverride = serde_json::from_str(
            r#"{"single_order_usdt": 250.0, "maker_recenter_trigger_bps": 1.5}"#,
        )
        .unwrap();
        assert!(!override_cfg.is_empty());
        let applied = override_cfg.apply_to(&defaults);
        assert_eq!(applied.single_order_usdt, 250.0);
        assert_eq!(applied.maker_recenter_trigger_bps, 1.5);
        assert_eq!(applied.max_open_usdt, defaults.max_open_usdt);
        override_cfg.validate(&defaults).unwrap();
        assert!(ChaseExecConfigOverride::default()
            .validate(&defaults)
            .is_err());
    }
}
