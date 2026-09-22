use crate::cta_special::config::{CtaSpecialConfig, CTA_SPECIAL_MAX_MODEL_AGE_MS};
use crate::strategy::cta_special_strategy::CtaSpecialExitConfig;
use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use trade_signal::model_output_hub::ModelOutputHub;

const CONFIG_RELOAD_INTERVAL_US: i64 = 1_000_000;
const MAX_FUTURE_MODEL_AGE_US: i64 = 5_000_000;
const MODEL_OUTPUT_MAX_SUBSCRIBERS: usize = 32;

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
    model_service: String,
    venue: TradingVenue,
    symbols: HashSet<String>,
    _node: Node<ipc::Service>,
    model_hub: ModelOutputHub,
    last_model_ts: HashMap<String, i64>,
    last_reload_check_us: i64,
}

impl CtaSpecialFactorChannel {
    pub fn new(config_path: impl AsRef<Path>) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let config = CtaSpecialConfig::load(&config_path)?;
        let venue = config.venue();
        let model_service = config.model_service();
        let node = NodeBuilder::new()
            .name(&NodeName::new("cta_special_pre_trade_factor")?)
            .create::<ipc::Service>()?;
        let mut model_hub =
            ModelOutputHub::new_with_max_subscribers(venue, MODEL_OUTPUT_MAX_SUBSCRIBERS);
        anyhow::ensure!(
            model_hub.update_services(&node, vec![model_service.clone()]) == 1,
            "failed to subscribe CTA special model service {}",
            model_service
        );
        let config_modified = fs::metadata(&config_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        let symbols = config.symbol_set();
        info!(
            "CTA special pre-trade factor channel ready rule={} service={} symbols={}",
            config.rule_name,
            model_service,
            symbols.len()
        );
        Ok(Self {
            config_path,
            config_modified,
            config,
            model_service,
            venue,
            symbols,
            _node: node,
            model_hub,
            last_model_ts: HashMap::new(),
            last_reload_check_us: 0,
        })
    }

    pub fn venue(&self) -> TradingVenue {
        self.venue
    }

    pub fn poll_updates(&mut self) -> Vec<CtaSpecialFactorUpdate> {
        self.maybe_reload_config();
        let now_us = get_timestamp_us();
        let mut updates = Vec::new();
        for event in self.model_hub.poll_updates() {
            if event.service_name != self.model_service || !self.symbols.contains(&event.symbol_key)
            {
                continue;
            }
            let lookup =
                self.model_hub
                    .cached_score(&self.model_service, &event.symbol_key, self.venue);
            if lookup.score_ts_ms <= 0
                || self
                    .last_model_ts
                    .get(&event.symbol_key)
                    .is_some_and(|last| lookup.score_ts_ms <= *last)
            {
                continue;
            }
            self.last_model_ts
                .insert(event.symbol_key.clone(), lookup.score_ts_ms);
            if !model_bar_is_fresh(lookup.score_ts_ms, now_us, CTA_SPECIAL_MAX_MODEL_AGE_MS) {
                warn!(
                    "drop stale CTA special pre-trade factor symbol={} model_ts_ms={}",
                    event.symbol_key, lookup.score_ts_ms
                );
                continue;
            }
            updates.push(CtaSpecialFactorUpdate {
                symbol: event.symbol_key,
                venue: self.venue,
                model_ts_ms: lookup.score_ts_ms,
                score_quantile: lookup.score_quantile,
                score_ready: lookup.score_ready,
                exit_config: exit_config(&self.config),
            });
        }
        updates
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
                if next.rule_name != self.config.rule_name {
                    warn!(
                        "CTA special pre-trade config reload rejected immutable rule_name change; redeploy to change factor"
                    );
                    self.config_modified = modified;
                    return;
                }
                self.symbols = next.symbol_set();
                self.config = next;
                self.config_modified = modified;
                info!(
                    "CTA special pre-trade config reloaded rule={} service={} symbols={} enabled={}",
                    self.config.rule_name,
                    self.model_service,
                    self.symbols.len(),
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
        rule_name: config.rule_name.clone(),
        factor_exit_enabled: config.execution.factor_exit_enabled,
        factor_exit_quantile_long: config.execution.factor_exit_quantile_long,
        factor_exit_quantile_short: config.execution.factor_exit_quantile_short,
        trailing_stop_enabled: config.execution.trailing_stop_enabled,
        trailing_stop_trigger_step: config.execution.trailing_stop_trigger_step,
        trailing_stop_move_step: config.execution.trailing_stop_move_step,
        max_holding_seconds: config.execution.max_holding_seconds,
    }
}

fn model_bar_is_fresh(model_ts_ms: i64, now_us: i64, max_age_ms: u64) -> bool {
    if model_ts_ms <= 0 {
        return false;
    }
    let model_ts_us = model_ts_ms.saturating_mul(1_000);
    let max_age_us = (max_age_ms as i64).saturating_mul(1_000);
    model_ts_us <= now_us.saturating_add(MAX_FUTURE_MODEL_AGE_US)
        && now_us.saturating_sub(model_ts_us) <= max_age_us
}

#[cfg(test)]
mod tests {
    use super::model_bar_is_fresh;

    #[test]
    fn model_freshness_rejects_old_and_future_bars() {
        let now_us = 1_000_000_000;
        assert!(model_bar_is_fresh(940_000, now_us, 120_000));
        assert!(!model_bar_is_fresh(879_999, now_us, 120_000));
        assert!(!model_bar_is_fresh(1_006_000, now_us, 120_000));
        assert!(!model_bar_is_fresh(0, now_us, 120_000));
    }
}
