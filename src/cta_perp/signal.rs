use super::recovery::{PositionRecovery, Positions, StrategyPosition, REQUEST_PATH};
use crate::cta_perp::config::{
    CtaPerpConfig, PositionAlgorithm, CTA_PERP_MAX_QUOTE_AGE_MS, CTA_PERP_SIGNAL_POLL_INTERVAL_MS,
    CTA_PERP_STATUS_PATH, CTA_PERP_VENUE_NAME, SYMBOL_MEAN_STRATEGY,
};
use crate::cta_perp::factor_channel::{PositionObservation, WeightedFactorChannel};
use crate::cta_perp::factors::{runtime_factor_config_key, SymbolFactorTable, WeightedBar};
use crate::cta_perp::journal::{GridPublication, SignalJournal, SIGNAL_JOURNAL_DIR};
use crate::cta_perp::positions::{full_qty, next_position_flag};
use crate::cta_position_publisher::{PositionPublisher, Publication};
use anyhow::{ensure, Context, Result};
use log::{info, warn};
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use trade_signal::MktChannel;

const SHADOW_PATH: &str = "run/cta_perp_position_flags.json";

fn strategy_scope(
    table: &SymbolFactorTable,
    algorithm: PositionAlgorithm,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut scope: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (symbol, row) in table {
        for (factor, _) in row
            .factors
            .iter()
            .zip(&row.weights)
            .filter(|(_, w)| **w != 0.0)
        {
            let name = if algorithm == PositionAlgorithm::V1 {
                SYMBOL_MEAN_STRATEGY
            } else {
                factor
            };
            scope
                .entry(name.to_string())
                .or_default()
                .insert(symbol.clone());
        }
    }
    scope
}

// A factor ledger and a symbol ledger cannot silently inherit one another's fills.
fn validate_position_scope(positions: &Positions, algorithm: PositionAlgorithm) -> Result<()> {
    for (strategy, rows) in positions {
        let compatible = (strategy == SYMBOL_MEAN_STRATEGY) == (algorithm == PositionAlgorithm::V1);
        ensure!(compatible || rows.values().all(|p| p.flag == 0 && p.qty == 0.0),
            "position algorithm cannot switch while {strategy} holds a position; close existing positions first");
    }
    Ok(())
}

pub struct CtaPerpSignalApp {
    config_path: PathBuf,
    config_modified: Option<SystemTime>,
    config: CtaPerpConfig,
    factors: WeightedFactorChannel,
    publisher: PositionPublisher,
    strategy_prefix: String,
    publication_error: Option<String>,
    configuration_error: Option<String>,
    positions: Positions,
    recovery: Option<PositionRecovery>,
    recovered_ts: BTreeMap<(String, String), i64>,
    recovery_error: Option<String>,
    observations: BTreeMap<String, BTreeMap<String, Value>>,
    journal: SignalJournal,
    last_reload_check_us: i64,
    entry_live_after_ms: i64,
}
impl CtaPerpSignalApp {
    pub async fn new(config_path: impl AsRef<Path>) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let config = CtaPerpConfig::load(&config_path)?;
        let factor_key = runtime_factor_config_key()?;
        let env = factor_key
            .strip_suffix(":cta_symbol_factors")
            .context("invalid CTA factor key")?
            .to_string();
        let factors = WeightedFactorChannel::new(factor_key, "cta_perp_model_input", false).await?;
        MktChannel::init_bbo_singleton_readonly(config.venue(), config.venue())
            .context("initialize CTA sizing BBO")?;
        let publisher = PositionPublisher::from_env()?;
        ensure!(
            !config.enabled || !publisher.is_empty(),
            "CTA_MANAGER_API_URLS (or CTA_MANAGER_API_URL) is required when publishing positions"
        );
        let positions = if Path::new(SHADOW_PATH).exists() {
            serde_json::from_slice(&fs::read(SHADOW_PATH)?)
                .context("read CTA observation positions")?
        } else {
            Positions::new()
        };
        let recovery = PositionRecovery::load(&config, factors.table(), &positions)?;
        if recovery.is_none() {
            validate_position_scope(&positions, config.position_algorithm)?;
        }
        let config_modified = fs::metadata(&config_path).and_then(|m| m.modified()).ok();
        let mut app = Self {
            config_path,
            config_modified,
            config,
            factors,
            publisher,
            strategy_prefix: std::env::var("CTA_POSITION_STRATEGY_PREFIX")
                .unwrap_or_else(|_| format!("{env}_")),
            publication_error: None,
            configuration_error: None,
            positions,
            recovery,
            recovered_ts: BTreeMap::new(),
            recovery_error: None,
            observations: BTreeMap::new(),
            journal: SignalJournal::new(SIGNAL_JOURNAL_DIR)?,
            last_reload_check_us: 0,
            entry_live_after_ms: get_timestamp_us() / 1000,
        };
        app.sync_factor_scope();
        app.write_status()?;
        info!(
            "CTA perp position strategies ready strategies={} enabled={}",
            strategy_scope(app.factors.table(), app.config.position_algorithm).len(),
            app.config.enabled
        );
        Ok(app)
    }
    fn sync_factor_scope(&mut self) {
        let scope = strategy_scope(self.factors.table(), self.config.position_algorithm);
        // Keep retired quantities as zero targets so Manager clears their original strategies.
        for (factor, rows) in &mut self.positions {
            for (symbol, position) in rows {
                let active = scope
                    .get(factor)
                    .is_some_and(|symbols| symbols.contains(symbol));
                if !active {
                    *position = StrategyPosition::default();
                }
            }
        }
    }
    fn publish_targets(&mut self) -> Result<()> {
        self.record_deliveries()?;
        if !self.config.enabled || self.recovery.is_some() || self.recovery_error.is_some() {
            self.publisher.pause();
            return Ok(());
        }
        ensure!(
            !self.publisher.is_empty(),
            "CTA Manager publication URL is required"
        );
        let mut publications = BTreeMap::new();
        let scope = strategy_scope(self.factors.table(), self.config.position_algorithm);
        let names: BTreeSet<String> = scope
            .keys()
            .cloned()
            .chain(self.positions.keys().cloned())
            .collect();
        for factor in names {
            let mut targets: BTreeMap<String, Value> = self
                .positions
                .get(&factor)
                .into_iter()
                .flat_map(|rows| rows.iter())
                .map(|(symbol, p)| (symbol.clone(), target_value(p.qty)))
                .collect();
            for symbol in scope.get(&factor).into_iter().flatten() {
                targets
                    .entry(symbol.clone())
                    .or_insert_with(|| target_value(0.0));
            }
            let payload = json!({"strategy_name":format!("{}{factor}",self.strategy_prefix),"targets":targets});
            publications.insert(factor.clone(), Publication {
                payload,
                context: json!({"factor":factor,"observations":self.observations.get(&factor),
                    "entry":self.config.entry,"execution":self.config.execution,"max_pos_u":self.config.max_pos_u}),
            });
        }
        self.publisher.submit(publications);
        Ok(())
    }
    fn record_deliveries(&mut self) -> Result<()> {
        for delivery in self.publisher.drain_deliveries() {
            let payload = &delivery.publication.payload;
            let context = &delivery.publication.context;
            let Some(factor) = context["factor"].as_str() else {
                continue;
            };
            let Some(targets) = payload["targets"].as_object() else {
                continue;
            };
            let now = delivery.published_ts_us;
            for (symbol, target) in targets {
                if let Some(row) = context["observations"].get(symbol) {
                    self.journal.record(symbol, row["model_ts_ms"].as_i64().unwrap_or(0), now,
                        "published", "manager_target", json!({"enabled":true,"strategy_name":factor,
                            "position_flag":row["position_flag"],"position_qty":row["position_qty"],
                            "signal":row["signal"],"position_algorithm":row["position_algorithm"],
                            "entry":row["entry"],"execution":row["execution"],"max_pos_u":row["max_pos_u"]}),
                        GridPublication { target:Some(json!({"strategy_name":payload["strategy_name"],"qty":target["qty"],"published_ts_us":now,"manager_url":delivery.manager_url})), ..Default::default() });
                }
            }
        }
        Ok(())
    }
    pub async fn run(&mut self) -> Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_millis(CTA_PERP_SIGNAL_POLL_INTERVAL_MS));
        loop {
            interval.tick().await;
            let now = get_timestamp_us();
            if self.recovery.is_some() {
                ensure!(
                    fs::metadata(&self.config_path)
                        .and_then(|m| m.modified())
                        .ok()
                        == self.config_modified,
                    "signal configuration changed during recovery; restart before publishing"
                );
                self.factors.poll_positions(&self.config);
                let result = self.recovery.as_mut().unwrap().poll(
                    &self.config,
                    self.factors.table(),
                    now / 1000,
                );
                match result {
                    Ok(true) => self.finish_recovery()?,
                    Ok(false) => {}
                    Err(error) => {
                        self.recovery_error = Some(format!("{error:#}"));
                        self.write_status()?;
                        return Err(error.context("position recovery failed; no targets published"));
                    }
                }
                if self.recovery.is_some() {
                    if now - self.last_reload_check_us >= 1_000_000 {
                        self.last_reload_check_us = now;
                        self.write_status()?;
                    }
                    continue;
                }
            }
            if now - self.last_reload_check_us >= 1_000_000 {
                self.last_reload_check_us = now;
                self.reload_config();
                self.sync_factor_scope();
                self.publication_error = self
                    .publish_targets()
                    .err()
                    .map(|error| format!("{error:#}"))
                    .or_else(|| self.publisher.error());
                if let Some(error) = &self.publication_error {
                    warn!("CTA publication failed; retry next refresh: {error}");
                }
                self.write_status()?;
            }
            let (changed, updates) = self.factors.poll_positions(&self.config);
            if changed {
                self.entry_live_after_ms = now / 1000;
            }
            for observation in updates {
                if let Err(error) = self.handle_observation(observation).await {
                    warn!("CTA factor observation failed: {error:#}");
                }
            }
            if changed {
                if let Err(error) = self.write_status() {
                    warn!("CTA status write failed: {error:#}");
                }
            }
        }
    }
    fn finish_recovery(&mut self) -> Result<()> {
        let recovered = self.recovery.take().context("missing position recovery")?;
        self.positions = recovered.positions;
        for (factor, rows) in recovered.bars {
            for (symbol, bar) in rows {
                let p = &self.positions[&factor][&symbol];
                let effective = self.config.for_symbol(&symbol);
                self.recovered_ts
                    .insert((factor.clone(), symbol.clone()), bar.model_ts_ms);
                self.observations.entry(factor.clone()).or_default().insert(symbol.clone(), json!({
                    "model_ts_ms":bar.model_ts_ms,"position_algorithm":effective.position_algorithm,
                    "entry":effective.entry,"execution":effective.execution,"max_pos_u":effective.max_pos_u,
                    "score_ready":true,"position_flag":p.flag,"position_qty":p.qty,"entry_notional":p.entry_notional,
                    "signal":signal_snapshot(&bar,p.flag),"updated_ts_us":get_timestamp_us(),"recovered":true}));
            }
        }
        self.sync_factor_scope();
        self.entry_live_after_ms = 0;
        self.publisher.reset();
        self.write_status()?;
        // Leave a durable audit, and do not replay again after a normal signal restart.
        fs::rename(REQUEST_PATH, "run/cta_perp_recovery_completed.json")?;
        info!(
            "CTA silent position recovery complete observations={}; final targets ready",
            recovered.records
        );
        Ok(())
    }
    fn reload_config(&mut self) {
        let modified = fs::metadata(&self.config_path)
            .and_then(|m| m.modified())
            .ok();
        if modified.is_none() || modified == self.config_modified {
            return;
        }
        match CtaPerpConfig::load(&self.config_path) {
            Ok(next) => {
                if let Err(error) =
                    validate_position_scope(&self.positions, next.position_algorithm)
                {
                    warn!("CTA algorithm switch rejected: {error:#}");
                    self.configuration_error = Some(format!("{error:#}"));
                    return;
                }
                if next.position_algorithm != self.config.position_algorithm {
                    self.observations.clear();
                    self.publisher.reset();
                }
                if next.enabled && !self.config.enabled {
                    self.publisher.reset();
                    self.entry_live_after_ms = get_timestamp_us() / 1000;
                }
                if next.enabled && self.publisher.is_empty() {
                    warn!("CTA publish enable rejected: CTA_MANAGER_API_URLS (or CTA_MANAGER_API_URL) is required");
                    return;
                }
                self.configuration_error = None;
                self.config = next;
                self.config_modified = modified;
                self.entry_live_after_ms = get_timestamp_us() / 1000;
            }
            Err(error) => warn!("CTA config refresh rejected: {error:#}"),
        }
    }
    async fn handle_observation(&mut self, observation: PositionObservation) -> Result<()> {
        let PositionObservation {
            symbol,
            factor,
            model_ts_ms,
            bar,
        } = observation;
        if self
            .recovered_ts
            .get(&(factor.clone(), symbol.clone()))
            .is_some_and(|ts| model_ts_ms <= *ts)
        {
            return Ok(());
        }
        let now = get_timestamp_us();
        let effective = self.config.for_symbol(&symbol);
        let mut position = self
            .positions
            .get(&factor)
            .and_then(|rows| rows.get(&symbol))
            .cloned()
            .unwrap_or_default();
        let mut flag = bar
            .as_ref()
            .map(|bar| next_position_flag(position.flag, bar, &effective))
            .unwrap_or(position.flag);
        let mut reason = if bar.is_none() {
            "factor_unready"
        } else {
            "hold"
        };
        if self.config.enabled && model_ts_ms <= self.entry_live_after_ms {
            flag = position.flag;
            reason = "historical_bar";
        }
        if flag != position.flag {
            if flag != 0 {
                let quote = MktChannel::instance()
                    .get_quote(&symbol, self.config.venue())
                    .context("missing full-position sizing BBO")?;
                ensure!(
                    quote.ts > 0
                        && quote.ts <= now
                        && now - quote.ts <= CTA_PERP_MAX_QUOTE_AGE_MS as i64 * 1000,
                    "stale sizing BBO"
                );
                let amount = self.config.max_pos_u;
                position.entry_notional = amount;
                position.qty =
                    full_qty(amount, flag, if flag > 0 { quote.ask } else { quote.bid })?;
                reason = "full_position_entry";
            } else {
                position.qty = 0.0;
                position.entry_notional = 0.0;
                reason = "factor_exit";
            }
            position.flag = flag;
        }
        let signal = bar.as_ref().map(|bar| signal_snapshot(bar, position.flag));
        let snapshot = json!({"enabled":self.config.enabled,"source":if effective.position_algorithm == PositionAlgorithm::V1 { "symbol_mean_strategy" } else { "independent_factor_strategy" },"strategy_name":factor,
            "position_algorithm":effective.position_algorithm,
            "entry":effective.entry,"execution":effective.execution,"max_pos_u":self.config.max_pos_u,"signal":signal,
            "position_flag":position.flag,"position_qty":position.qty});
        self.observations.entry(factor.clone()).or_default().insert(symbol.clone(), json!({"model_ts_ms":model_ts_ms,
            "position_algorithm":effective.position_algorithm,"entry":effective.entry,"execution":effective.execution,"max_pos_u":self.config.max_pos_u,
            "score_ready":bar.is_some(),"position_flag":position.flag,"position_qty":position.qty,"entry_notional":position.entry_notional,"signal":signal,"updated_ts_us":now}));
        self.positions
            .entry(factor)
            .or_default()
            .insert(symbol.clone(), position);
        self.journal.record(
            &symbol,
            model_ts_ms,
            now,
            "observed",
            reason,
            snapshot,
            GridPublication::default(),
        );
        self.write_status()
    }
    fn write_status(&self) -> Result<()> {
        fs::create_dir_all("run")?;
        let mut strategies = self.observations.clone();
        let now = get_timestamp_us();
        for (symbol, row) in self.factors.table() {
            let quote = MktChannel::instance().get_quote(symbol, self.config.venue());
            let mark = quote
                .filter(|q| {
                    q.ts > 0 && q.ts <= now && now - q.ts <= CTA_PERP_MAX_QUOTE_AGE_MS as i64 * 1000
                })
                .map(|q| (q.bid + q.ask) / 2.0)
                .filter(|p| p.is_finite() && *p > 0.0);
            let names = strategy_scope(
                &BTreeMap::from([(symbol.clone(), row.clone())]),
                self.config.position_algorithm,
            );
            for factor in names.keys() {
                let item = strategies.entry(factor.clone()).or_default().entry(symbol.clone())
                    .or_insert_with(|| json!({"score_ready":false,"position_flag":0,"position_qty":0.0,"entry_notional":0.0}));
                let qty = item["position_qty"].as_f64().unwrap_or(0.0);
                item["factor_sign"] = if factor == SYMBOL_MEAN_STRATEGY {
                    json!(null)
                } else {
                    let weight = row
                        .factors
                        .iter()
                        .position(|f| f == factor)
                        .map(|i| row.weights[i])
                        .unwrap_or(0.0);
                    json!(if weight > 0.0 { "+" } else { "-" })
                };
                item["factor_count"] = json!(row.weights.iter().filter(|w| **w != 0.0).count());
                item["allocation_usdt"] = json!(self.config.max_pos_u);
                item["mark_price"] = json!(mark);
                item["position_usdt"] = if qty == 0.0 {
                    json!(0.0)
                } else {
                    json!(mark.map(|price| qty * price))
                };
            }
        }
        let status = json!({"enabled":self.config.enabled,"journal_healthy":self.journal.healthy(),"venue":CTA_PERP_VENUE_NAME,
            "position_algorithm":self.config.position_algorithm,"symbol_overrides":self.config.symbol_overrides,
            "recovering":self.recovery.is_some(),"recovery_records":self.recovery.as_ref().map(|r|r.records),"recovery_error":self.recovery_error,
            "configuration_error":self.configuration_error,"publication_error":self.publication_error,"manager_publications":self.publisher.statuses(),"manager_strategy_prefix":self.strategy_prefix,
            "virtual_account_name":std::env::var("CTA_VIRTUAL_ACCOUNT_NAME").ok(),
            "follower_source_id":std::env::var("CTA_FOLLOWER_SOURCE_ID").ok(),
            "factor_config_key":self.factors.key(),"symbol_factors":self.factors.table(),"max_pos_u":self.config.max_pos_u,"allocation":self.config.position_algorithm.allocation(),"strategies":strategies,"updated_ts_us":now,"scheduled_entries":0});
        atomic_json(Path::new(CTA_PERP_STATUS_PATH), &status)?;
        if self.recovery.is_none() {
            atomic_json(Path::new(SHADOW_PATH), &self.positions)?;
        }
        Ok(())
    }
}
fn target_value(qty: f64) -> Value {
    if qty == 0.0 {
        json!({"qty":"clear","signal":0})
    } else {
        json!({"qty":qty,"signal":0})
    }
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))
}
fn signal_snapshot(bar: &WeightedBar, flag: i8) -> Value {
    json!({"model_ts_ms":bar.model_ts_ms,"score":bar.value,"quantile":bar.quantile,"decision_value":bar.decision_value,"score_ready":true,"execution_coefficient":1.0,
        "factor_votes":bar.votes,"position_flag":flag,"decision":if flag > 0 { "long" } else if flag < 0 { "short" } else { "flat" },
        "nq_long_value":bar.nq.filter_long_value,"nq_short_value":bar.nq.filter_short_value,
        "nq_long_quantile":bar.nq.filter_long_quantile,"nq_short_quantile":bar.nq.filter_short_quantile,"filter_ready":bar.nq.filter_ready})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_targets_are_explicit_clears() {
        assert_eq!(target_value(0.0)["qty"], "clear");
        assert_eq!(target_value(-0.0)["qty"], "clear");
        assert_eq!(target_value(-2.0)["qty"], -2.0);
    }

    #[test]
    fn strategy_scope_is_per_factor_or_one_mean_for_each_symbol() {
        let table = crate::cta_perp::factors::parse_factor_table(
            r#"{
            "ETHUSDT":{"factor_036":1,"factor_051":-1},
            "BTCUSDT":{"factor_036":0,"factor_051":1}
        }"#,
        )
        .unwrap();
        let independent = strategy_scope(&table, PositionAlgorithm::V0);
        assert_eq!(independent.len(), 2);
        assert_eq!(independent["factor_036"].len(), 1);
        let mean = strategy_scope(&table, PositionAlgorithm::V1);
        assert_eq!(mean.len(), 1);
        assert_eq!(
            mean[SYMBOL_MEAN_STRATEGY],
            BTreeSet::from(["BTCUSDT".into(), "ETHUSDT".into()])
        );
    }

    #[test]
    fn switching_cannot_reassign_held_positions_or_net_opposing_factors() {
        let mut positions: Positions = serde_json::from_value(json!({
            "factor_036":{"ETHUSDT":{"flag":1,"qty":2.0,"entry_notional":200.0}},
            "factor_051":{"ETHUSDT":{"flag":-1,"qty":-2.0,"entry_notional":200.0}}
        }))
        .unwrap();
        assert!(validate_position_scope(&positions, PositionAlgorithm::V0).is_ok());
        assert!(validate_position_scope(&positions, PositionAlgorithm::V1).is_err());
        for rows in positions.values_mut() {
            for position in rows.values_mut() {
                *position = StrategyPosition::default();
            }
        }
        assert!(validate_position_scope(&positions, PositionAlgorithm::V1).is_ok());
        positions.insert(
            SYMBOL_MEAN_STRATEGY.into(),
            BTreeMap::from([(
                "ETHUSDT".into(),
                StrategyPosition {
                    flag: 1,
                    qty: 2.0,
                    entry_notional: 200.0,
                },
            )]),
        );
        assert!(validate_position_scope(&positions, PositionAlgorithm::V1).is_ok());
        assert!(validate_position_scope(&positions, PositionAlgorithm::V0).is_err());
    }
}
