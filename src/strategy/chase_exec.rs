use crate::strategy::batch_exec_strategy::{validate_target_signal, BatchExecTarget};
use order_common::TradingVenue;
use serde::{Deserialize, Serialize};

pub const CHASE_EXEC_POSITION_CLOSE_STRATEGY_NAME: &str = "SYSTEM_POSITION_CLOSE";

const fn default_maker_amend_cooldown_ms() -> u32 {
    1_000
}

/// ChaseExec configuration: own-best post-only batches that follow the
/// same-side BBO via in-place amend, with fill-driven (water-level) release.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChaseExecConfig {
    /// Strategy-wide order action limit across all symbols; 0 disables the
    /// 60-second window. New orders and amendments both consume it.
    #[serde(default)]
    pub strategy_order_rate_limit_per_min: u32,
    /// Strategy-wide order action limit across all symbols; 0 disables the
    /// 10-second window. New orders and amendments both consume it.
    #[serde(default)]
    pub strategy_order_rate_limit_10s: u32,
    /// Lower bound used when sizing a target generation's Chase batch.
    pub batch_floor_usdt: f64,
    /// Maximum number of batches used to size one target generation.
    pub max_batch: u32,
    /// Maximum number of batch-equivalents left unfilled at once.
    pub max_open_batches: u32,
    /// Anchor movement (bps of own best) required before a live child is
    /// amended. 0 amends whenever the aligned price actually changes.
    pub maker_recenter_trigger_bps: f64,
    /// Per-child minimum delay between amend requests.
    #[serde(default = "default_maker_amend_cooldown_ms")]
    pub maker_amend_cooldown_ms: u32,
    /// Maker child lifetime in seconds; on expiry the remainder escalates to
    /// taker.
    pub maker_timeout_sec: u32,
    pub target_tolerance_usdt: f64,
}

impl Default for ChaseExecConfig {
    fn default() -> Self {
        Self {
            strategy_order_rate_limit_per_min: 0,
            strategy_order_rate_limit_10s: 0,
            batch_floor_usdt: 100.0,
            max_batch: 4,
            max_open_batches: 2,
            maker_recenter_trigger_bps: 5.0,
            maker_amend_cooldown_ms: default_maker_amend_cooldown_ms(),
            maker_timeout_sec: 120,
            target_tolerance_usdt: 10.0,
        }
    }
}

impl ChaseExecConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.batch_floor_usdt.is_finite() || self.batch_floor_usdt <= 0.0 {
            return Err("batch_floor_usdt must be positive".to_string());
        }
        if self.max_batch == 0 {
            return Err("max_batch must be positive".to_string());
        }
        if self.max_open_batches == 0 || self.max_open_batches > self.max_batch {
            return Err("max_open_batches must be in the range 1..=max_batch".to_string());
        }
        if !self.maker_recenter_trigger_bps.is_finite() || self.maker_recenter_trigger_bps < 0.0 {
            return Err("maker_recenter_trigger_bps must be finite and non-negative".to_string());
        }
        if self.maker_timeout_sec == 0 {
            return Err("maker_timeout_sec must be positive".to_string());
        }
        if !self.target_tolerance_usdt.is_finite() || self.target_tolerance_usdt < 0.0 {
            return Err("target_tolerance_usdt must be finite and non-negative".to_string());
        }
        Ok(())
    }

    pub fn effective_batch_usdt(&self, delta_usdt: f64) -> f64 {
        self.batch_floor_usdt
            .max(delta_usdt.abs() / f64::from(self.max_batch))
    }

    pub fn open_water_level_usdt(&self, effective_batch_usdt: f64) -> f64 {
        effective_batch_usdt * f64::from(self.max_open_batches)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChaseExecConfigOverride {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_floor_usdt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_open_batches: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_recenter_trigger_bps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_amend_cooldown_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_timeout_sec: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_tolerance_usdt: Option<f64>,
}

impl ChaseExecConfigOverride {
    pub fn is_empty(&self) -> bool {
        self.batch_floor_usdt.is_none()
            && self.max_batch.is_none()
            && self.max_open_batches.is_none()
            && self.maker_recenter_trigger_bps.is_none()
            && self.maker_amend_cooldown_ms.is_none()
            && self.maker_timeout_sec.is_none()
            && self.target_tolerance_usdt.is_none()
    }

    pub fn apply_to(&self, defaults: &ChaseExecConfig) -> ChaseExecConfig {
        ChaseExecConfig {
            strategy_order_rate_limit_per_min: defaults.strategy_order_rate_limit_per_min,
            strategy_order_rate_limit_10s: defaults.strategy_order_rate_limit_10s,
            batch_floor_usdt: self.batch_floor_usdt.unwrap_or(defaults.batch_floor_usdt),
            max_batch: self.max_batch.unwrap_or(defaults.max_batch),
            max_open_batches: self.max_open_batches.unwrap_or(defaults.max_open_batches),
            maker_recenter_trigger_bps: self
                .maker_recenter_trigger_bps
                .unwrap_or(defaults.maker_recenter_trigger_bps),
            maker_amend_cooldown_ms: self
                .maker_amend_cooldown_ms
                .unwrap_or(defaults.maker_amend_cooldown_ms),
            maker_timeout_sec: self.maker_timeout_sec.unwrap_or(defaults.maker_timeout_sec),
            target_tolerance_usdt: self
                .target_tolerance_usdt
                .unwrap_or(defaults.target_tolerance_usdt),
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
        let config = ChaseExecConfig::default();
        config.validate().unwrap();
        assert_eq!(config.maker_amend_cooldown_ms, 1_000);
        assert_eq!(config.strategy_order_rate_limit_per_min, 0);
        assert_eq!(config.strategy_order_rate_limit_10s, 0);
    }

    #[test]
    fn config_rejects_bad_values() {
        let mut cfg = ChaseExecConfig::default();
        cfg.batch_floor_usdt = 0.0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.max_batch = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.max_open_batches = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.max_open_batches = cfg.max_batch + 1;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.maker_recenter_trigger_bps = -0.5;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.maker_timeout_sec = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = ChaseExecConfig::default();
        cfg.target_tolerance_usdt = -1.0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn override_applies_and_validates() {
        let defaults = ChaseExecConfig::default();
        let override_cfg: ChaseExecConfigOverride = serde_json::from_str(
            r#"{"batch_floor_usdt": 250.0, "max_open_batches": 1, "maker_recenter_trigger_bps": 1.5}"#,
        )
        .unwrap();
        assert!(!override_cfg.is_empty());
        let applied = override_cfg.apply_to(&defaults);
        assert_eq!(applied.batch_floor_usdt, 250.0);
        assert_eq!(applied.max_open_batches, 1);
        assert_eq!(applied.maker_recenter_trigger_bps, 1.5);
        assert_eq!(applied.max_batch, defaults.max_batch);
        assert_eq!(
            applied.strategy_order_rate_limit_per_min,
            defaults.strategy_order_rate_limit_per_min
        );
        override_cfg.validate(&defaults).unwrap();
        assert!(ChaseExecConfigOverride::default()
            .validate(&defaults)
            .is_err());
        assert!(serde_json::from_str::<ChaseExecConfigOverride>(
            r#"{"strategy_order_rate_limit_10s": 10}"#
        )
        .is_err());
    }

    #[test]
    fn omitted_strategy_rate_limits_default_to_disabled() {
        let config: ChaseExecConfig = serde_json::from_str(
            r#"{
                "batch_floor_usdt": 100.0,
                "max_batch": 4,
                "max_open_batches": 2,
                "maker_recenter_trigger_bps": 5.0,
                "maker_amend_cooldown_ms": 1000,
                "maker_timeout_sec": 120,
                "target_tolerance_usdt": 10.0
            }"#,
        )
        .unwrap();
        assert_eq!(config.strategy_order_rate_limit_per_min, 0);
        assert_eq!(config.strategy_order_rate_limit_10s, 0);
    }

    #[test]
    fn target_generation_sizes_batches_and_open_water_level() {
        let cfg = ChaseExecConfig::default();
        assert_eq!(cfg.effective_batch_usdt(10_000.0), 2_500.0);
        assert_eq!(cfg.open_water_level_usdt(2_500.0), 5_000.0);
        assert_eq!(cfg.effective_batch_usdt(80.0), 100.0);
    }
}
