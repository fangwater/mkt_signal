use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use order_common::TradingVenue;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use runtime_common::time_util::get_timestamp_us;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use trade_signal::cta_config::{
    cta_rules_redis_key, cta_strategy_params_redis_key, CtaExecOverrides, CtaRule, CtaRuleSet,
};
use trade_signal::model_output_hub::ModelOutputHub;

const RELOAD_INTERVAL: Duration = Duration::from_secs(60);
const MAX_MODEL_AGE_US: i64 = 120_000_000;

pub struct CtaFactorUpdate {
    pub symbol: String,
    pub model_ts_ms: i64,
    pub quantile: Option<f64>,
    pub ready: bool,
    pub exit_long: f64,
    pub exit_short: f64,
}

pub struct CtaFactorChannel {
    redis: RedisSettings,
    env: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    node: Node<ipc::Service>,
    model_hub: ModelOutputHub,
    rule: Option<CtaRule>,
    last_model_ts: HashMap<String, i64>,
    last_reload: Instant,
}

impl CtaFactorChannel {
    pub async fn new(
        redis: RedisSettings,
        env: String,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new("cta_pre_trade_factor")?)
            .create::<ipc::Service>()?;
        let mut channel = Self {
            redis,
            env,
            open_venue,
            hedge_venue,
            node,
            model_hub: ModelOutputHub::new_with_max_subscribers(hedge_venue, 32),
            rule: None,
            last_model_ts: HashMap::new(),
            last_reload: Instant::now(),
        };
        channel.reload().await?;
        anyhow::ensure!(
            channel.rule.is_some(),
            "CTA pre-trade requires an active rule in {} before startup",
            cta_rules_redis_key(&channel.env)
        );
        Ok(channel)
    }

    async fn reload(&mut self) -> Result<()> {
        let mut redis = RedisClient::connect(self.redis.clone()).await?;
        let key = cta_rules_redis_key(&self.env);
        let Some(raw) = redis.get_string(&key).await? else {
            warn!("CTA factor config absent: {}", key);
            return Ok(());
        };
        let strategy_key =
            cta_strategy_params_redis_key(&self.env, self.open_venue, self.hedge_venue);
        let fields = redis.hgetall_map(&strategy_key).await?;
        let exec = CtaExecOverrides::from_strategy_params(&fields, &strategy_key)?;
        let rules = CtaRuleSet::parse_with_exec(&raw, Some(&exec))?;
        let Some(next) = rules.rules().first() else {
            return Ok(());
        };
        if let Some(current) = &self.rule {
            anyhow::ensure!(
                current.model_service == next.model_service && current.rule_id == next.rule_id,
                "CTA model_service/rule_id cannot change while pre-trade is running"
            );
        } else {
            anyhow::ensure!(
                self.model_hub
                    .update_services(&self.node, vec![next.model_service.clone()])
                    == 1,
                "CTA pre-trade model subscription failed: {}",
                next.model_service
            );
        }
        info!(
            "CTA pre-trade factor subscribed service={} rule={}",
            next.model_service, next.rule_id
        );
        self.rule = Some(next.clone());
        Ok(())
    }

    pub async fn poll_updates(&mut self) -> Vec<CtaFactorUpdate> {
        if self.last_reload.elapsed() >= RELOAD_INTERVAL {
            self.last_reload = Instant::now();
            if let Err(error) = self.reload().await {
                warn!("CTA pre-trade factor reload rejected: {error:#}");
            }
        }
        let Some(rule) = &self.rule else {
            return Vec::new();
        };
        let now_us = get_timestamp_us();
        let mut updates = Vec::new();
        for event in self.model_hub.poll_updates() {
            if event.service_name != rule.model_service {
                continue;
            }
            let lookup = self.model_hub.cached_score(
                &rule.model_service,
                &event.symbol_key,
                self.hedge_venue,
            );
            let model_ts_ms = lookup.score_ts_ms;
            if model_ts_ms <= 0
                || self
                    .last_model_ts
                    .get(&event.symbol_key)
                    .is_some_and(|ts| model_ts_ms <= *ts)
            {
                continue;
            }
            self.last_model_ts
                .insert(event.symbol_key.clone(), model_ts_ms);
            let model_us = model_ts_ms.saturating_mul(1_000);
            if model_us > now_us.saturating_add(5_000_000)
                || now_us.saturating_sub(model_us) > MAX_MODEL_AGE_US
            {
                warn!(
                    "CTA pre-trade dropped stale factor symbol={} ts={model_ts_ms}",
                    event.symbol_key
                );
                continue;
            }
            updates.push(CtaFactorUpdate {
                symbol: event.symbol_key,
                model_ts_ms,
                quantile: lookup.score_quantile,
                ready: lookup.score_ready,
                exit_long: rule.factor_exit_quantile_long,
                exit_short: rule.factor_exit_quantile_short,
            });
        }
        updates
    }
}
