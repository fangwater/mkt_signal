//! Volume budgets are in base quantity, including outstanding batch reservations.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecAlgorithm {
    #[default]
    Batch,
    Pov,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PovLiquidity {
    MakerOnly,
    TakerOnly,
    #[default]
    MakerThenTaker,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PovConfig {
    pub participation_rate: f64,
    pub max_batch_usdt: f64,
    pub max_carry_usdt: f64,
    pub volume_stale_ms: u32,
    pub quote_stale_ms: u32,
    pub duration_ms: u32,
    pub liquidity: PovLiquidity,
    pub limit_price: Option<f64>,
}

impl Default for PovConfig {
    fn default() -> Self {
        Self {
            participation_rate: 0.1,
            max_batch_usdt: 300.0,
            max_carry_usdt: 600.0,
            volume_stale_ms: 5_000,
            quote_stale_ms: 1_000,
            duration_ms: 3_600_000,
            liquidity: PovLiquidity::MakerThenTaker,
            limit_price: None,
        }
    }
}

impl PovConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.participation_rate.is_finite()
            || self.participation_rate <= 0.0
            || self.participation_rate > 1.0
        {
            return Err("pov.participation_rate must be in (0, 1]".into());
        }
        for (name, value) in [
            ("max_batch_usdt", self.max_batch_usdt),
            ("max_carry_usdt", self.max_carry_usdt),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(format!("pov.{name} must be finite and positive"));
            }
        }
        if self.max_carry_usdt < self.max_batch_usdt {
            return Err("pov.max_carry_usdt must be >= max_batch_usdt".into());
        }
        if self.volume_stale_ms == 0 || self.quote_stale_ms == 0 || self.duration_ms == 0 {
            return Err("pov timeouts and duration must be positive".into());
        }
        if self.limit_price.is_some_and(|p| !p.is_finite() || p <= 0.0) {
            return Err("pov.limit_price must be finite and positive".into());
        }
        // The existing market-order transport cannot enforce an exchange price cap.
        if self.limit_price.is_some() && self.liquidity != PovLiquidity::MakerOnly {
            return Err("pov.limit_price requires maker_only liquidity".into());
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct PovState {
    pub started_at_us: i64,
    pub last_trade_at_us: i64,
    pub market_base_qty: f64,
    pub filled_base_qty: f64,
    pub credit_base_qty: f64,
}

impl PovState {
    pub fn reset(&mut self, now_us: i64) {
        *self = Self {
            started_at_us: now_us,
            ..Self::default()
        };
    }

    pub fn observe(
        &mut self,
        config: &PovConfig,
        ts_us: i64,
        now_us: i64,
        qty: f64,
        price: f64,
        reserved: f64,
    ) {
        if self.started_at_us <= 0
            || ts_us < self.started_at_us
            || ts_us > now_us
            || now_us.saturating_sub(ts_us) > i64::from(config.volume_stale_ms) * 1_000
            || !qty.is_finite()
            || qty <= 0.0
            || !price.is_finite()
            || price <= 0.0
            || self.expired(config, now_us)
        {
            return;
        }
        self.last_trade_at_us = self.last_trade_at_us.max(ts_us);
        self.market_base_qty += qty;
        self.credit_base_qty = (self.credit_base_qty + qty * config.participation_rate)
            .min(reserved + config.max_carry_usdt / price);
    }

    pub fn fill(&mut self, qty: f64) {
        self.filled_base_qty += qty;
        self.credit_base_qty -= qty;
    }

    pub fn available(&self, reserved: f64) -> f64 {
        (self.credit_base_qty - reserved).max(0.0)
    }

    pub fn batch_qty(
        &self,
        config: &PovConfig,
        reserved: f64,
        desired: f64,
        ask: f64,
        minimum: f64,
    ) -> Option<f64> {
        if !desired.is_finite()
            || desired <= 0.0
            || !ask.is_finite()
            || ask <= 0.0
            || !minimum.is_finite()
            || minimum <= 0.0
        {
            return None;
        }
        let qty = desired
            .min(self.available(reserved))
            .min(config.max_batch_usdt / ask);
        (qty >= minimum).then_some(qty)
    }

    pub fn expired(&self, config: &PovConfig, now_us: i64) -> bool {
        self.started_at_us > 0
            && now_us.saturating_sub(self.started_at_us) >= i64::from(config.duration_ms) * 1_000
    }

    pub fn fresh(&self, config: &PovConfig, now_us: i64) -> bool {
        self.last_trade_at_us > 0
            && now_us >= self.last_trade_at_us
            && now_us - self.last_trade_at_us <= i64::from(config.volume_stale_ms) * 1_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_fills_and_requotes_share_one_budget() {
        let config = PovConfig::default();
        let mut state = PovState::default();
        state.reset(1_000_000);
        state.observe(&config, 2_000_000, 2_000_000, 20.0, 100.0, 0.0);
        assert_eq!(state.available(0.0), 2.0);
        assert_eq!(state.available(2.0), 0.0);
        state.fill(0.5);
        assert_eq!(state.available(1.5), 0.0);
        assert_eq!(state.available(0.0), 1.5);
        assert_eq!(state.filled_base_qty, 0.5);
    }

    #[test]
    fn stale_future_pre_target_and_expired_volume_do_not_fund_orders() {
        let config = PovConfig::default();
        let mut state = PovState::default();
        state.reset(10_000_000);
        for (ts, now) in [
            (9_000_000, 10_000_000),
            (12_000_000, 11_000_000),
            (11_000_000, 20_000_000),
            (4_000_000_000, 4_000_000_000),
        ] {
            state.observe(&config, ts, now, 100.0, 100.0, 0.0);
        }
        assert_eq!(state.available(0.0), 0.0);
        assert!(!state.fresh(&config, 20_000_000));
    }

    #[test]
    fn carry_is_bounded_without_erasing_reservations() {
        let config = PovConfig::default();
        let mut state = PovState::default();
        state.reset(1);
        state.observe(&config, 2, 2, 1000.0, 100.0, 2.0);
        assert_eq!(state.available(2.0), 6.0);
    }

    #[test]
    fn venue_minimum_waits_for_volume_and_caps_never_round_up() {
        let config = PovConfig::default();
        let mut state = PovState::default();
        state.reset(1);
        state.observe(&config, 2, 2, 1.0, 100.0, 0.0);
        assert_eq!(state.batch_qty(&config, 0.0, 100.0, 100.0, 0.2), None);
        state.observe(&config, 3, 3, 1.0, 100.0, 0.0);
        assert_eq!(state.batch_qty(&config, 0.0, 100.0, 100.0, 0.2), Some(0.2));
        state.observe(&config, 4, 4, 100.0, 100.0, 0.0);
        assert_eq!(state.batch_qty(&config, 0.0, 100.0, 100.0, 0.2), Some(3.0));
        assert_eq!(state.batch_qty(&config, 0.0, 0.25, 100.0, 0.2), Some(0.25));
        assert_eq!(state.batch_qty(&config, 0.0, 100.0, 100.0, 4.0), None);
    }

    #[test]
    fn execution_simulation_bounds_filled_plus_live_by_observed_participation() {
        let config = PovConfig::default();
        let mut state = PovState::default();
        state.reset(1);
        let mut live = 0.0;
        for step in 2..200 {
            let volume = (step % 13) as f64 / 10.0;
            state.observe(&config, step, step, volume, 100.0, live);
            if step % 3 == 0 {
                let fill = live / 2.0;
                state.fill(fill);
                live -= fill;
            }
            if step % 7 == 0 {
                live = 0.0;
            }
            if let Some(qty) = state.batch_qty(&config, live, 100.0, 100.0, 0.2) {
                live += qty;
            }
            assert!(
                state.filled_base_qty + live
                    <= state.market_base_qty * config.participation_rate + 1e-10
            );
        }
    }
}
