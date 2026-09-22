use crate::cta_special::config::{
    CtaSpecialConfig, CTA_SPECIAL_MAX_MODEL_AGE_MS, CTA_SPECIAL_MAX_QUOTE_AGE_MS,
    CTA_SPECIAL_SIGNAL_POLL_INTERVAL_MS, CTA_SPECIAL_STATUS_PATH, CTA_SPECIAL_VENUE_NAME,
};
use anyhow::{Context, Result};
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use ipc_common::iceoryx_publisher::TradeSignalPublisher;
use log::{info, warn};
use order_common::{OrderType, Side, TradingVenue};
use quote_plan::quote_plan_levels::{build_quote_plan_levels, QuotePlanLevelSpec};
use runtime_common::exchange::Exchange;
use runtime_common::execution_backend::{rapidx_portfolio_id, ExecBackend};
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use signal_common::common::{SignalBytes, TradingLeg};
use signal_common::open_signal::ArbOpenCtx;
use signal_common::trade_signal::SignalType;
use signal_common::venue_min_qty_table::VenueMinQtyTable;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use trade_signal::model_output_hub::{ModelOutputHub, ModelOutputScoreLookupResult};
use trade_signal::MktChannel;

const SIGNAL_CHANNEL: &str = "trade_signal";
const MAX_DELAYED_SIGNAL_AGE_US: i64 = 5_000_000;
const MODEL_OUTPUT_MAX_SUBSCRIBERS: usize = 32;
const BAR_MS: i64 = 60_000;

#[derive(Debug, Clone)]
struct ScheduledEntry {
    symbol: String,
    side: Side,
    due_ts_us: i64,
    model_ts_ms: i64,
    config: CtaSpecialConfig,
}

#[derive(Debug, Clone, Serialize)]
struct SymbolStatus {
    model_ts_ms: i64,
    score: Option<f64>,
    quantile: Option<f64>,
    long_threshold: Option<f64>,
    short_threshold: Option<f64>,
    score_ready: bool,
    nq_long_value: Option<f64>,
    nq_long_threshold: Option<f64>,
    nq_short_value: Option<f64>,
    nq_short_threshold: Option<f64>,
    filter_ready: bool,
    decision: &'static str,
    updated_ts_us: i64,
}

#[derive(Debug, Serialize)]
struct RuntimeStatus<'a> {
    enabled: bool,
    venue: &'a str,
    rule_name: &'a str,
    model_service: &'a str,
    updated_ts_us: i64,
    scheduled_entries: usize,
    symbols: &'a HashMap<String, SymbolStatus>,
}

pub struct CtaSpecialSignalApp {
    config_path: PathBuf,
    config_modified: Option<SystemTime>,
    config: CtaSpecialConfig,
    model_service: String,
    venue: TradingVenue,
    _model_node: Node<ipc::Service>,
    model_hub: ModelOutputHub,
    signal_pub: TradeSignalPublisher,
    min_qty_table: VenueMinQtyTable,
    symbols: HashSet<String>,
    last_model_ts: HashMap<String, i64>,
    last_scheduled_entry_ts_us: HashMap<String, i64>,
    scheduled: Vec<ScheduledEntry>,
    status: HashMap<String, SymbolStatus>,
    last_reload_check_us: i64,
    entry_live_after_ms: i64,
}

impl CtaSpecialSignalApp {
    pub async fn new(config_path: impl AsRef<Path>) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let config = CtaSpecialConfig::load(&config_path)?;
        let venue = config.venue();
        let model_service = config.model_service();
        anyhow::ensure!(
            ExecBackend::for_exchange(Exchange::Binance)? == ExecBackend::Ltp,
            "cta_special requires binance=ltp execution backend"
        );
        let _ = rapidx_portfolio_id().context("cta_special requires LTP_PORTFOLIO_ID")?;

        MktChannel::init_bbo_singleton_readonly(venue, venue)
            .context("initialize CTA special BBO subscriber")?;
        let model_node = NodeBuilder::new()
            .name(&NodeName::new("cta_special_model_input")?)
            .create::<ipc::Service>()?;
        let mut model_hub =
            ModelOutputHub::new_with_max_subscribers(venue, MODEL_OUTPUT_MAX_SUBSCRIBERS);
        anyhow::ensure!(
            model_hub.update_services(&model_node, vec![model_service.clone()]) == 1,
            "failed to subscribe CTA special model service {}",
            model_service
        );
        let signal_pub = TradeSignalPublisher::open_or_create(SIGNAL_CHANNEL)
            .context("open CTA special trade signal publisher")?;
        let mut min_qty_table = VenueMinQtyTable::new(venue);
        min_qty_table
            .refresh()
            .await
            .context("load Binance futures order filters for CTA special")?;
        let config_modified = fs::metadata(&config_path)
            .and_then(|metadata| metadata.modified())
            .ok();
        let symbols = config.symbol_set();
        info!(
            "CtaSpecialSignal ready rule={} service={} symbols={} ltp_only=true",
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
            _model_node: model_node,
            model_hub,
            signal_pub,
            min_qty_table,
            symbols,
            last_model_ts: HashMap::new(),
            last_scheduled_entry_ts_us: HashMap::new(),
            scheduled: Vec::new(),
            status: HashMap::new(),
            last_reload_check_us: 0,
            entry_live_after_ms: get_timestamp_us().div_euclid(1_000),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_millis(CTA_SPECIAL_SIGNAL_POLL_INTERVAL_MS));
        loop {
            interval.tick().await;
            self.maybe_reload_config();
            self.poll_models();
            self.publish_due_entries();
        }
    }

    fn maybe_reload_config(&mut self) {
        let now_us = get_timestamp_us();
        if now_us.saturating_sub(self.last_reload_check_us) < 1_000_000 {
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
                        "CTA special config reload rejected immutable rule_name change; redeploy to change factor"
                    );
                    self.config_modified = modified;
                    return;
                }
                if publisher_threshold_config_changed(&self.config, &next) {
                    self.entry_live_after_ms = now_us.div_euclid(1_000).saturating_add(BAR_MS);
                    info!(
                        "CTA special publisher-owned quantiles changed; pausing entry until model_ts_ms>{}",
                        self.entry_live_after_ms
                    );
                }
                self.symbols = next.symbol_set();
                self.scheduled.clear();
                self.config = next;
                self.config_modified = modified;
                info!(
                    "CTA special config reloaded rule={} service={} symbols={} enabled={}",
                    self.config.rule_name,
                    self.model_service,
                    self.symbols.len(),
                    self.config.enabled
                );
            }
            Err(err) => warn!("CTA special config reload rejected: {err:#}"),
        }
    }

    fn poll_models(&mut self) {
        let events = self.model_hub.poll_updates();
        for event in events {
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
            if !model_bar_is_fresh(
                lookup.score_ts_ms,
                get_timestamp_us(),
                CTA_SPECIAL_MAX_MODEL_AGE_MS,
            ) {
                continue;
            }
            self.handle_model_bar(&event.symbol_key, lookup);
        }
    }

    fn handle_model_bar(&mut self, symbol: &str, lookup: ModelOutputScoreLookupResult) {
        let now_us = get_timestamp_us();
        let decision = entry_decision(&self.config, &lookup);
        self.status.insert(
            symbol.to_string(),
            SymbolStatus {
                model_ts_ms: lookup.score_ts_ms,
                score: lookup.score,
                quantile: lookup.score_quantile,
                long_threshold: lookup.score_long_threshold,
                short_threshold: lookup.score_short_threshold,
                score_ready: lookup.score_ready,
                nq_long_value: lookup.filter_long_value,
                nq_long_threshold: lookup.filter_long_threshold,
                nq_short_value: lookup.filter_short_value,
                nq_short_threshold: lookup.filter_short_threshold,
                filter_ready: lookup.filter_ready,
                decision: match decision {
                    Some(Side::Buy) => "long",
                    Some(Side::Sell) => "short",
                    None => "flat",
                },
                updated_ts_us: now_us,
            },
        );
        let Some(side) = decision.filter(|_| {
            self.config.enabled && entry_bar_is_live(lookup.score_ts_ms, self.entry_live_after_ms)
        }) else {
            self.write_status();
            return;
        };
        let model_ts_us = lookup.score_ts_ms.saturating_mul(1_000);
        let cooldown_us = self.config.entry.cooldown_seconds.saturating_mul(1_000_000);
        if cooldown_us > 0
            && self
                .last_scheduled_entry_ts_us
                .get(symbol)
                .is_some_and(|last| model_ts_us.saturating_sub(*last) < cooldown_us)
        {
            self.write_status();
            return;
        }
        self.last_scheduled_entry_ts_us
            .insert(symbol.to_string(), model_ts_us);
        let due_ts_us = lookup
            .score_ts_ms
            .saturating_mul(1_000)
            .saturating_add(
                self.config
                    .entry
                    .signal_delay_seconds
                    .saturating_mul(1_000_000),
            )
            .max(now_us);
        self.scheduled.push(ScheduledEntry {
            symbol: symbol.to_string(),
            side,
            due_ts_us,
            model_ts_ms: lookup.score_ts_ms,
            config: self.config.clone(),
        });
        self.write_status();
    }

    fn publish_due_entries(&mut self) {
        let now_us = get_timestamp_us();
        let previous_count = self.scheduled.len();
        let mut remaining = Vec::with_capacity(self.scheduled.len());
        for scheduled in self.scheduled.drain(..) {
            if scheduled.due_ts_us > now_us {
                remaining.push(scheduled);
                continue;
            }
            if now_us.saturating_sub(scheduled.due_ts_us) > MAX_DELAYED_SIGNAL_AGE_US {
                warn!(
                    "drop stale CTA special delayed entry symbol={} model_ts_ms={} age_us={}",
                    scheduled.symbol,
                    scheduled.model_ts_ms,
                    now_us.saturating_sub(scheduled.due_ts_us)
                );
                continue;
            }
            if let Err(err) = publish_entry_grid(
                &self.signal_pub,
                &self.min_qty_table,
                self.venue,
                &scheduled,
                now_us,
            ) {
                warn!(
                    "publish CTA special entry grid failed symbol={} side={} err={:#}",
                    scheduled.symbol,
                    scheduled.side.as_str(),
                    err
                );
            }
        }
        self.scheduled = remaining;
        if self.scheduled.len() != previous_count {
            self.write_status();
        }
    }

    fn write_status(&self) {
        let path = Path::new(CTA_SPECIAL_STATUS_PATH);
        if let Some(parent) = path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                warn!("create CTA special status directory failed: {err}");
                return;
            }
        }
        let status = RuntimeStatus {
            enabled: self.config.enabled,
            venue: CTA_SPECIAL_VENUE_NAME,
            rule_name: &self.config.rule_name,
            model_service: &self.model_service,
            updated_ts_us: get_timestamp_us(),
            scheduled_entries: self.scheduled.len(),
            symbols: &self.status,
        };
        let Ok(payload) = serde_json::to_vec_pretty(&status) else {
            return;
        };
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, payload).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }
}

fn model_bar_is_fresh(model_ts_ms: i64, now_us: i64, max_age_ms: u64) -> bool {
    if model_ts_ms <= 0 {
        return false;
    }
    let model_ts_us = model_ts_ms.saturating_mul(1_000);
    let max_age_us = (max_age_ms as i64).saturating_mul(1_000);
    model_ts_us <= now_us.saturating_add(MAX_DELAYED_SIGNAL_AGE_US)
        && now_us.saturating_sub(model_ts_us) <= max_age_us
}

fn entry_bar_is_live(model_ts_ms: i64, process_started_ms: i64) -> bool {
    model_ts_ms > process_started_ms
}

fn publisher_threshold_config_changed(old: &CtaSpecialConfig, new: &CtaSpecialConfig) -> bool {
    old.entry.factor_long_quantile != new.entry.factor_long_quantile
        || old.entry.factor_short_quantile != new.entry.factor_short_quantile
        || old.entry.nq_long_quantile != new.entry.nq_long_quantile
        || old.entry.nq_short_quantile != new.entry.nq_short_quantile
}

fn entry_decision(
    config: &CtaSpecialConfig,
    lookup: &ModelOutputScoreLookupResult,
) -> Option<Side> {
    if !lookup.score_ready {
        return None;
    }
    let score = lookup.score.filter(|value| value.is_finite())?;
    let long_threshold = lookup
        .score_long_threshold
        .filter(|value| value.is_finite())?;
    let short_threshold = lookup
        .score_short_threshold
        .filter(|value| value.is_finite())?;
    let long_signal = config.allows_long() && score > long_threshold;
    let short_signal = config.allows_short() && score < short_threshold;
    if long_signal == short_signal {
        return None;
    }
    if config.entry.nq_change_enabled {
        if !lookup.filter_ready {
            return None;
        }
        if long_signal
            && !matches!(
                (lookup.filter_long_value, lookup.filter_long_threshold),
                (Some(value), Some(threshold)) if value.is_finite() && threshold.is_finite() && value >= threshold
            )
        {
            return None;
        }
        if short_signal
            && !matches!(
                (lookup.filter_short_value, lookup.filter_short_threshold),
                (Some(value), Some(threshold)) if value.is_finite() && threshold.is_finite() && value <= threshold
            )
        {
            return None;
        }
    }
    if long_signal {
        Some(Side::Buy)
    } else {
        Some(Side::Sell)
    }
}

fn publish_entry_grid(
    publisher: &TradeSignalPublisher,
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    scheduled: &ScheduledEntry,
    now_us: i64,
) -> Result<()> {
    let quote = MktChannel::instance()
        .get_quote(&scheduled.symbol, venue)
        .with_context(|| format!("missing BBO for {}", scheduled.symbol))?;
    anyhow::ensure!(
        quote.ts > 0
            && now_us.saturating_sub(quote.ts)
                <= (CTA_SPECIAL_MAX_QUOTE_AGE_MS as i64).saturating_mul(1_000),
        "stale BBO ts={} now={}",
        quote.ts,
        now_us
    );
    let base_price = if scheduled.side == Side::Buy {
        quote.bid
    } else {
        quote.ask
    };
    let specs: Vec<QuotePlanLevelSpec> = scheduled
        .config
        .execution
        .open_offsets
        .iter()
        .enumerate()
        .map(|(index, offset)| QuotePlanLevelSpec {
            side: scheduled.side,
            side_level_index: index + 1,
            offset: *offset,
            base_price,
        })
        .collect();
    let (price_tick, qty_tick, levels) = build_quote_plan_levels(
        venue,
        &scheduled.symbol,
        scheduled.config.execution.order_notional_usdt,
        &specs,
        table,
    )
    .map_err(anyhow::Error::msg)?;
    for level in levels {
        let mut ctx = ArbOpenCtx::new();
        ctx.opening_leg = TradingLeg::new_with_qty(
            venue,
            quote.bid,
            quote.bid_qty,
            quote.ask,
            quote.ask_qty,
            quote.ts,
        );
        ctx.hedging_leg = ctx.opening_leg;
        ctx.set_opening_symbol(&scheduled.symbol);
        ctx.set_hedging_symbol(&scheduled.symbol);
        ctx.set_side(scheduled.side);
        ctx.set_order_type(OrderType::Limit);
        anyhow::ensure!(
            ctx.set_price_with_tick_floor(level.aligned_price, price_tick)
                && ctx.set_amount_with_tick_floor(level.aligned_qty, qty_tick),
            "failed to quantize level {}",
            level.side_level_index
        );
        ctx.create_ts = now_us;
        ctx.exp_time = now_us.saturating_add(
            scheduled
                .config
                .execution
                .maker_ttl_seconds
                .saturating_mul(1_000_000),
        );
        ctx.price_offset = level.offset;
        ctx.spread_rate = 0.0;
        ctx.hedge_timeout_us = 0;
        ctx.set_from_key(
            format!(
                "cta_special=1:cta_rule={}:cta_factor_exit={}:cta_exit_long={}:cta_exit_short={}:cta_trailing={}:cta_trigger={}:cta_move={}:cta_max_hold_s={}:model_ts_ms={}:level={}",
                scheduled.config.rule_name,
                u8::from(scheduled.config.execution.factor_exit_enabled),
                scheduled.config.execution.factor_exit_quantile_long,
                scheduled.config.execution.factor_exit_quantile_short,
                u8::from(scheduled.config.execution.trailing_stop_enabled),
                scheduled.config.execution.trailing_stop_trigger_step,
                scheduled.config.execution.trailing_stop_move_step,
                scheduled.config.execution.max_holding_seconds,
                scheduled.model_ts_ms,
                level.side_level_index,
            )
            .into_bytes(),
        );
        publisher.publish_trade_signal_parts(
            SignalType::ArbOpen,
            now_us,
            0.0,
            ctx.to_bytes().as_ref(),
        )?;
    }
    info!(
        "CTA special entry grid published symbol={} side={} levels={} model_ts_ms={}",
        scheduled.symbol,
        scheduled.side.as_str(),
        specs.len(),
        scheduled.model_ts_ms
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cta_special::config::{EntryConfig, ExecutionConfig};

    fn config(nq: bool) -> CtaSpecialConfig {
        CtaSpecialConfig {
            enabled: true,
            rule_name: "tp_vpi_018".to_string(),
            symbols: vec!["BTCUSDT".to_string()],
            entry: EntryConfig {
                nq_change_enabled: nq,
                ..EntryConfig::default()
            },
            execution: ExecutionConfig {
                order_notional_usdt: 100.0,
                factor_exit_quantile_long: 0.3,
                factor_exit_quantile_short: 0.7,
                trailing_stop_trigger_step: 0.02,
                trailing_stop_move_step: 0.01,
                ..ExecutionConfig::default()
            },
        }
    }

    fn lookup() -> ModelOutputScoreLookupResult {
        ModelOutputScoreLookupResult {
            service_name: "model_output/one-binance-futures-1m-tp_vpi_018".to_string(),
            symbol_key: "BTCUSDT".to_string(),
            subscribed: true,
            score: Some(2.0),
            score_quantile: Some(0.95),
            score_ready: true,
            score_long_threshold: Some(1.0),
            score_short_threshold: Some(-1.0),
            filter_long_value: Some(0.2),
            filter_long_threshold: Some(0.1),
            filter_short_value: Some(-0.1),
            filter_short_threshold: Some(-0.2),
            filter_ready: true,
            score_ts_ms: 60_000,
            note: "ok".to_string(),
        }
    }

    #[test]
    fn long_requires_strict_zscore_threshold_and_nq_gate() {
        let mut value = lookup();
        assert_eq!(entry_decision(&config(true), &value), Some(Side::Buy));
        value.filter_long_value = Some(0.05);
        assert_eq!(entry_decision(&config(true), &value), None);
        assert_eq!(entry_decision(&config(false), &value), Some(Side::Buy));
        value.score = value.score_long_threshold;
        assert_eq!(entry_decision(&config(false), &value), None);
    }

    #[test]
    fn entry_decision_respects_configured_trade_sides() {
        let value = lookup();
        let mut short_only = config(false);
        short_only.entry.trade_sides = "short".to_string();
        assert_eq!(entry_decision(&short_only, &value), None);

        let mut long_only = config(false);
        long_only.entry.trade_sides = "long".to_string();
        assert_eq!(entry_decision(&long_only, &value), Some(Side::Buy));
    }

    #[test]
    fn publisher_quantile_change_requires_a_fresh_bar() {
        let old = config(true);
        let mut new = old.clone();
        assert!(!publisher_threshold_config_changed(&old, &new));
        new.entry.factor_long_quantile = 0.95;
        assert!(publisher_threshold_config_changed(&old, &new));
    }

    #[test]
    fn historical_catchup_bars_cannot_become_live_entries() {
        let now_us = 1_000_000_000;
        assert!(model_bar_is_fresh(940_000, now_us, 120_000));
        assert!(!model_bar_is_fresh(879_999, now_us, 120_000));
        assert!(!model_bar_is_fresh(1_006_000, now_us, 120_000));
        assert!(!model_bar_is_fresh(0, now_us, 120_000));
    }

    #[test]
    fn cached_bar_at_signal_start_is_not_entry_eligible() {
        let started_ms = 1_000_000;
        assert!(!entry_bar_is_live(999_999, started_ms));
        assert!(!entry_bar_is_live(started_ms, started_ms));
        assert!(entry_bar_is_live(1_000_001, started_ms));
    }
}
