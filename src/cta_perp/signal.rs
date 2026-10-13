use crate::cta_perp::config::{
    CtaPerpConfig, CTA_PERP_MAX_QUOTE_AGE_MS, CTA_PERP_SIGNAL_POLL_INTERVAL_MS,
    CTA_PERP_STATUS_PATH, CTA_PERP_VENUE_NAME,
};
use crate::cta_perp::factor_channel::{IndependentFactorObservation, WeightedFactorChannel};
use crate::cta_perp::factors::{runtime_factor_config_key, WeightedBar};
use crate::cta_perp::journal::{GridPublication, SignalJournal, SIGNAL_JOURNAL_DIR};
use crate::cta_perp::positions::{full_qty, next_position_flag};
use anyhow::{ensure, Context, Result};
use log::{info, warn};
use runtime_common::time_util::get_timestamp_us;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use trade_signal::MktChannel;

const SHADOW_PATH: &str = "run/cta_perp_position_flags.json";
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StrategyPosition {
    flag: i8,
    qty: f64,
    #[serde(default)]
    entry_notional: f64,
}
type Positions = BTreeMap<String, BTreeMap<String, StrategyPosition>>;

pub struct CtaPerpSignalApp {
    config_path: PathBuf,
    config_modified: Option<SystemTime>,
    config: CtaPerpConfig,
    factors: WeightedFactorChannel,
    manager: reqwest::Client,
    manager_url: Option<String>,
    strategy_prefix: String,
    last_published: BTreeMap<String, Value>,
    publication_error: Option<String>,
    positions: Positions,
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
        let manager_url = std::env::var("CTA_MANAGER_API_URL").ok();
        if let Some(url) = &manager_url {
            let parsed = reqwest::Url::parse(url).context("invalid CTA Manager URL")?;
            ensure!(
                parsed.scheme() == "http" || parsed.scheme() == "https",
                "invalid Manager URL scheme"
            );
        }
        ensure!(
            !config.enabled || manager_url.is_some(),
            "CTA_MANAGER_API_URL is required when publishing positions"
        );
        let mut headers = reqwest::header::HeaderMap::new();
        if let Ok(token) = std::env::var("CTA_PUBLISH_TOKEN") {
            let mut value = reqwest::header::HeaderValue::from_str(&token)
                .map_err(|_| anyhow::anyhow!("invalid CTA publish token header"))?;
            value.set_sensitive(true);
            headers.insert("X-CTA-Publish-Token", value);
        }
        let manager = reqwest::Client::builder()
            .no_proxy()
            .default_headers(headers)
            .timeout(Duration::from_secs(5))
            .build()?;
        let positions = if Path::new(SHADOW_PATH).exists() {
            serde_json::from_slice(&fs::read(SHADOW_PATH)?)
                .context("read CTA observation positions")?
        } else {
            Positions::new()
        };
        let config_modified = fs::metadata(&config_path).and_then(|m| m.modified()).ok();
        let mut app = Self {
            config_path,
            config_modified,
            config,
            factors,
            manager,
            manager_url,
            strategy_prefix: std::env::var("CTA_POSITION_STRATEGY_PREFIX")
                .unwrap_or_else(|_| format!("{env}_")),
            last_published: BTreeMap::new(),
            publication_error: None,
            positions,
            observations: BTreeMap::new(),
            journal: SignalJournal::new(SIGNAL_JOURNAL_DIR)?,
            last_reload_check_us: 0,
            entry_live_after_ms: get_timestamp_us() / 1000,
        };
        app.sync_factor_scope();
        app.write_status()?;
        info!(
            "CTA perp independent factor strategies ready strategies={} enabled={}",
            app.factors
                .table()
                .values()
                .flat_map(|row| row.factors.iter())
                .collect::<BTreeSet<_>>()
                .len(),
            app.config.enabled
        );
        Ok(app)
    }
    fn sync_factor_scope(&mut self) {
        // Keep retired quantities as zero targets so Manager clears their original strategies.
        for (factor, rows) in &mut self.positions {
            for (symbol, position) in rows {
                let active = self.factors.table().get(symbol).is_some_and(|row| {
                    row.factors
                        .iter()
                        .zip(&row.weights)
                        .any(|(f, w)| f == factor && *w != 0.0)
                });
                if !active {
                    *position = StrategyPosition::default();
                }
            }
        }
    }
    async fn publish_targets(&mut self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        let base = self
            .manager_url
            .as_ref()
            .context("CTA_MANAGER_API_URL is required")?;
        let names: BTreeSet<String> = self
            .factors
            .table()
            .values()
            .flat_map(|row| {
                row.factors
                    .iter()
                    .zip(&row.weights)
                    .filter(|(_, w)| **w != 0.0)
                    .map(|(f, _)| f.clone())
            })
            .chain(self.positions.keys().cloned())
            .collect();
        for factor in names {
            let mut targets: BTreeMap<String, Value> = self
                .positions
                .get(&factor)
                .into_iter()
                .flat_map(|rows| rows.iter())
                .map(|(symbol, p)| (symbol.clone(), json!({"qty":p.qty,"signal":0})))
                .collect();
            for (symbol, row) in self.factors.table() {
                if row
                    .factors
                    .iter()
                    .zip(&row.weights)
                    .any(|(f, w)| f == &factor && *w != 0.0)
                {
                    targets
                        .entry(symbol.clone())
                        .or_insert_with(|| json!({"qty":0.0,"signal":0}));
                }
            }
            let payload = json!({"strategy_name":format!("{}{factor}",self.strategy_prefix),"targets":targets});
            if self.last_published.get(&factor) == Some(&payload) {
                continue;
            }
            let response = self
                .manager
                .post(format!(
                    "{}/catalog/position-strategies",
                    base.trim_end_matches('/')
                ))
                .json(&payload)
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("CTA Manager position publication unavailable"))?;
            ensure!(
                response.status().is_success(),
                "CTA Manager position publication failed: status={}",
                response.status()
            );
            let now = get_timestamp_us();
            for (symbol, target) in &targets {
                if let Some(row) = self
                    .observations
                    .get(&factor)
                    .and_then(|rows| rows.get(symbol))
                {
                    self.journal.record(symbol, row["model_ts_ms"].as_i64().unwrap_or(0), now,
                        "published", "manager_target", json!({"enabled":true,"strategy_name":factor,
                            "position_flag":row["position_flag"],"position_qty":row["position_qty"],
                            "signal":row["signal"],"entry":self.config.entry,"execution":self.config.execution,"max_pos_u":self.config.max_pos_u}),
                        GridPublication { target:Some(json!({"strategy_name":payload["strategy_name"],"qty":target["qty"],"published_ts_us":now})), ..Default::default() });
                }
            }
            self.last_published.insert(factor, payload);
        }
        Ok(())
    }
    pub async fn run(&mut self) -> Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_millis(CTA_PERP_SIGNAL_POLL_INTERVAL_MS));
        loop {
            interval.tick().await;
            let now = get_timestamp_us();
            if now - self.last_reload_check_us >= 1_000_000 {
                self.last_reload_check_us = now;
                self.reload_config();
                self.sync_factor_scope();
                self.publication_error = self
                    .publish_targets()
                    .await
                    .err()
                    .map(|error| format!("{error:#}"));
                if let Some(error) = &self.publication_error {
                    warn!("CTA publication failed; retry next refresh: {error}");
                }
                self.write_status()?;
            }
            let (changed, updates) = self.factors.poll_independent(&self.config.entry);
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
    fn reload_config(&mut self) {
        let modified = fs::metadata(&self.config_path)
            .and_then(|m| m.modified())
            .ok();
        if modified.is_none() || modified == self.config_modified {
            return;
        }
        match CtaPerpConfig::load(&self.config_path) {
            Ok(next) => {
                if next.enabled && !self.config.enabled {
                    self.last_published.clear();
                    self.entry_live_after_ms = get_timestamp_us() / 1000;
                }
                if next.enabled && self.manager_url.is_none() {
                    warn!("CTA publish enable rejected: CTA_MANAGER_API_URL is required");
                    return;
                }
                self.config = next;
                self.config_modified = modified;
                self.entry_live_after_ms = get_timestamp_us() / 1000;
            }
            Err(error) => warn!("CTA config refresh rejected: {error:#}"),
        }
    }
    async fn handle_observation(
        &mut self,
        observation: IndependentFactorObservation,
    ) -> Result<()> {
        let IndependentFactorObservation {
            symbol,
            factor,
            model_ts_ms,
            bar,
        } = observation;
        let now = get_timestamp_us();
        let mut position = self
            .positions
            .get(&factor)
            .and_then(|rows| rows.get(&symbol))
            .cloned()
            .unwrap_or_default();
        let mut flag = bar
            .as_ref()
            .map(|bar| next_position_flag(position.flag, bar, &self.config))
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
        let snapshot = json!({"enabled":self.config.enabled,"source":"independent_factor_strategy","strategy_name":factor,
            "entry":self.config.entry,"execution":self.config.execution,"max_pos_u":self.config.max_pos_u,"signal":signal,
            "position_flag":position.flag,"position_qty":position.qty});
        self.observations.entry(factor.clone()).or_default().insert(symbol.clone(), json!({"model_ts_ms":model_ts_ms,
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
            for (factor, weight) in row
                .factors
                .iter()
                .zip(&row.weights)
                .filter(|(_, w)| **w != 0.0)
            {
                let item = strategies.entry(factor.clone()).or_default().entry(symbol.clone())
                    .or_insert_with(|| json!({"score_ready":false,"position_flag":0,"position_qty":0.0,"entry_notional":0.0}));
                let qty = item["position_qty"].as_f64().unwrap_or(0.0);
                item["factor_sign"] = json!(if *weight > 0.0 { "+" } else { "-" });
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
            "publication_error":self.publication_error,"manager_strategy_prefix":self.strategy_prefix,
            "virtual_account_name":std::env::var("CTA_VIRTUAL_ACCOUNT_NAME").ok(),
            "follower_source_id":std::env::var("CTA_FOLLOWER_SOURCE_ID").ok(),
            "factor_config_key":self.factors.key(),"symbol_factors":self.factors.table(),"max_pos_u":self.config.max_pos_u,"allocation":"per_symbol_factor","strategies":strategies,"updated_ts_us":now,"scheduled_entries":0});
        atomic_json(Path::new(CTA_PERP_STATUS_PATH), &status)?;
        atomic_json(Path::new(SHADOW_PATH), &self.positions)
    }
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))
}
fn signal_snapshot(bar: &WeightedBar, flag: i8) -> Value {
    json!({"model_ts_ms":bar.model_ts_ms,"score":bar.value,"quantile":bar.quantile,"score_ready":true,"execution_coefficient":1.0,
        "factor_votes":bar.votes,"position_flag":flag,"decision":if flag > 0 { "long" } else if flag < 0 { "short" } else { "flat" },
        "nq_long_value":bar.nq.filter_long_value,"nq_short_value":bar.nq.filter_short_value,
        "nq_long_quantile":bar.nq.filter_long_quantile,"nq_short_quantile":bar.nq.filter_short_quantile,"filter_ready":bar.nq.filter_ready})
}
