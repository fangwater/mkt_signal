use crate::cta_special::config::CtaSpecialConfig;
use crate::cta_special::factor_channel::WeightedFactorChannel;
use crate::cta_special::factors::{runtime_factor_config_key, WEIGHTED_RULE_NAME};
use crate::strategy::cta_special_strategy::CtaSpecialExitConfig;
use anyhow::Result;
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const CONFIG_RELOAD_INTERVAL_US: i64 = 1_000_000;

#[derive(Debug, Clone)]
pub struct CtaSpecialFactorUpdate {
    pub symbol: String,
    pub venue: TradingVenue,
    pub model_ts_ms: i64,
    pub score_quantile: Option<f64>,
    pub score_ready: bool,
    pub exit_config: CtaSpecialExitConfig,
}

pub struct CtaSpecialFactorChannel {
    config_path: PathBuf,
    config_modified: Option<SystemTime>,
    config: CtaSpecialConfig,
    venue: TradingVenue,
    factors: WeightedFactorChannel,
    last_reload_check_us: i64,
}

impl CtaSpecialFactorChannel {
    pub async fn new(config_path: impl AsRef<Path>) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let config = CtaSpecialConfig::load(&config_path)?;
        let venue = config.venue();
        let factors = WeightedFactorChannel::new(
            runtime_factor_config_key()?,
            "cta_special_pre_trade_factor",
            true,
        )
        .await?;
        let config_modified = fs::metadata(&config_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        info!(
            "CTA special pre-trade factor channel ready key={} symbols={}",
            factors.key(),
            factors.table().len()
        );
        Ok(Self {
            config_path,
            config_modified,
            config,
            venue,
            factors,
            last_reload_check_us: 0,
        })
    }

    pub fn venue(&self) -> TradingVenue {
        self.venue
    }

    pub fn poll_updates(&mut self) -> Vec<CtaSpecialFactorUpdate> {
        self.maybe_reload_config();
        let (_, updates) = self.factors.poll(&self.config.entry);
        updates
            .into_iter()
            .map(|observation| CtaSpecialFactorUpdate {
                symbol: observation.symbol,
                venue: self.venue,
                model_ts_ms: observation.model_ts_ms,
                score_quantile: observation.bar.as_ref().map(|bar| bar.quantile),
                score_ready: observation.bar.is_some(),
                exit_config: exit_config(&self.config),
            })
            .collect()
    }

    fn maybe_reload_config(&mut self) {
        let now_us = get_timestamp_us();
        if now_us.saturating_sub(self.last_reload_check_us) < CONFIG_RELOAD_INTERVAL_US {
            return;
        }
        self.last_reload_check_us = now_us;
        let modified = fs::metadata(&self.config_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        if modified.is_none() || modified == self.config_modified {
            return;
        }
        match CtaSpecialConfig::load(&self.config_path) {
            Ok(next) => {
                self.config = next;
                self.config_modified = modified;
                info!(
                    "CTA special pre-trade execution config reloaded enabled={}",
                    self.config.enabled
                );
            }
            Err(err) => warn!(
                "CTA special pre-trade config reload rejected path={} err={err:#}",
                self.config_path.display()
            ),
        }
    }
}

fn exit_config(config: &CtaSpecialConfig) -> CtaSpecialExitConfig {
    CtaSpecialExitConfig {
        rule_name: WEIGHTED_RULE_NAME.into(),
        factor_exit_enabled: true,
        factor_exit_quantile_long: config.execution.factor_exit_quantile_long,
        factor_exit_quantile_short: config.execution.factor_exit_quantile_short,
        trailing_stop_enabled: false,
        trailing_stop_trigger_step: 0.02,
        trailing_stop_move_step: 0.01,
        max_holding_seconds: 0,
    }
}

#[cfg(test)]
mod tests {
    use crate::cta_special::factors::model_bar_is_fresh;

    #[test]
    fn model_freshness_rejects_old_and_future_bars() {
        let now_us = 1_000_000_000;
        assert!(model_bar_is_fresh(940_000, now_us, 120_000));
        assert!(!model_bar_is_fresh(879_999, now_us, 120_000));
        assert!(!model_bar_is_fresh(1_006_000, now_us, 120_000));
        assert!(!model_bar_is_fresh(0, now_us, 120_000));
    }
}
