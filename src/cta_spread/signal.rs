use crate::cta_spread::config::{
    CtaSpreadConfig, CTA_SPREAD_MAX_QUOTE_AGE_MS, CTA_SPREAD_SIGNAL_POLL_INTERVAL_MS,
    CTA_SPREAD_STATUS_PATH, CTA_SPREAD_VENUE_NAME,
};
use crate::cta_spread::factor_channel::{IndependentFactorObservation, WeightedFactorChannel};
use crate::cta_spread::factors::{runtime_factor_config_key, WeightedBar};
use crate::cta_spread::journal::{GridPublication, SignalJournal, SIGNAL_JOURNAL_DIR};
use crate::cta_spread::positions::{next_position_flag, StrategyPosition};
use anyhow::{ensure, Context, Result};
use log::{info, warn};
use order_common::TradingVenue;
use quote_plan::Quote;
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use trade_signal::MktChannel;

const SHADOW_PATH: &str = "run/cta_spread_position_flags.json";
type Positions = BTreeMap<String, BTreeMap<String, StrategyPosition>>;

pub struct CtaSpreadSignalApp {
    config_path: PathBuf,
    config_modified: Option<SystemTime>,
    config: CtaSpreadConfig,
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
impl CtaSpreadSignalApp {
    pub async fn new(config_path: impl AsRef<Path>) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let config = CtaSpreadConfig::load(&config_path)?;
        let factor_key = runtime_factor_config_key()?;
        let env = factor_key
            .strip_suffix(":cta_symbol_factors")
            .context("invalid CTA factor key")?
            .to_string();
        let factors =
            WeightedFactorChannel::new(factor_key, "cta_spread_model_input", false).await?;
        MktChannel::init_bbo_singleton_readonly(TradingVenue::BinanceMargin, config.venue())
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
        let positions: Positions = if Path::new(SHADOW_PATH).exists() {
            serde_json::from_slice(&fs::read(SHADOW_PATH)?)
                .context("read CTA observation positions")?
        } else {
            Positions::new()
        };
        for rows in positions.values() {
            for (symbol, position) in rows {
                cash_symbol(symbol)?;
                position
                    .validate()
                    .with_context(|| format!("invalid persisted CTA spread position {symbol}"))?;
            }
        }
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
            "CTA spread independent factor strategies ready strategies={} enabled={}",
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
        if !self.config.enabled {
            return;
        }
        // Removing a binding exits its direction but preserves both hedged legs.
        for (factor, rows) in &mut self.positions {
            for (symbol, position) in rows {
                let active = self.factors.table().get(symbol).is_some_and(|row| {
                    row.factors
                        .iter()
                        .zip(&row.weights)
                        .any(|(f, w)| f == factor && *w != 0.0)
                });
                if !active {
                    position.exit();
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
                .flat_map(|(symbol, p)| market_targets(symbol, p))
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
                        .or_insert_with(|| json!({"qty":0.0,"signal":1}));
                    targets
                        .entry(cash_symbol(symbol)?.to_string())
                        .or_insert_with(|| json!({"qty":0.0,"signal":0}));
                }
            }
            let payload = position_payload(format!("{}{factor}", self.strategy_prefix), &targets);
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
                        GridPublication { target:Some(json!({"strategy_name":payload["strategy_name"],"qty":target["qty"],"cash_qty":targets[cash_symbol(symbol)?]["qty"],"perpetual_qty":target["qty"],"published_ts_us":now})), ..Default::default() });
                }
            }
            self.last_published.insert(factor, payload);
        }
        Ok(())
    }
    pub async fn run(&mut self) -> Result<()> {
        let mut interval =
            tokio::time::interval(Duration::from_millis(CTA_SPREAD_SIGNAL_POLL_INTERVAL_MS));
        loop {
            interval.tick().await;
            let now = get_timestamp_us();
            if now - self.last_reload_check_us >= 1_000_000 {
                self.last_reload_check_us = now;
                self.reload_config();
                self.sync_factor_scope();
                self.write_status()?;
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
        match CtaSpreadConfig::load(&self.config_path) {
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
        let reason = advance_position(
            &mut position,
            bar.as_ref(),
            &self.config,
            model_ts_ms,
            self.entry_live_after_ms,
            now,
            MktChannel::instance().get_quote(&symbol, TradingVenue::BinanceMargin),
        )?;
        let signal = bar.as_ref().map(|bar| signal_snapshot(bar, position.flag));
        let snapshot = json!({"enabled":self.config.enabled,"source":"independent_factor_strategy","strategy_name":factor,
            "entry":self.config.entry,"execution":self.config.execution,"max_pos_u":self.config.max_pos_u,"signal":signal,"decision_reason":reason,
            "position_flag":position.flag,"position_qty":position.qty,"cash_qty":position.cash_qty,"perpetual_qty":position.perpetual_qty});
        self.observations.entry(factor.clone()).or_default().insert(symbol.clone(), json!({"model_ts_ms":model_ts_ms,
            "score_ready":bar.is_some(),"position_flag":position.flag,"position_qty":position.qty,"cash_qty":position.cash_qty,"perpetual_qty":position.perpetual_qty,"entry_notional":position.entry_notional,"signal":signal,"decision_reason":reason,"updated_ts_us":now}));
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
                    q.ts > 0
                        && q.ts <= now
                        && now - q.ts <= CTA_SPREAD_MAX_QUOTE_AGE_MS as i64 * 1000
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
                let qty = self
                    .positions
                    .get(factor)
                    .and_then(|rows| rows.get(symbol))
                    .map(|position| position.qty)
                    .unwrap_or(0.0);
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
        for (factor, positions) in &self.positions {
            for (symbol, position) in positions {
                let item = strategies
                    .entry(factor.clone())
                    .or_default()
                    .entry(symbol.clone())
                    .or_insert_with(|| json!({"score_ready":false}));
                item["position_flag"] = json!(position.flag);
                item["position_qty"] = json!(position.qty);
                item["cash_qty"] = json!(position.cash_qty);
                item["perpetual_qty"] = json!(position.perpetual_qty);
                item["entry_notional"] = json!(position.entry_notional);
            }
        }
        let status = json!({"enabled":self.config.enabled,"journal_healthy":self.journal.healthy(),"venue":CTA_SPREAD_VENUE_NAME,
            "publication_error":self.publication_error,"manager_strategy_prefix":self.strategy_prefix,
            "virtual_account_name":std::env::var("CTA_VIRTUAL_ACCOUNT_NAME").ok(),
            "follower_source_id":std::env::var("CTA_FOLLOWER_SOURCE_ID").ok(),
            "factor_config_key":self.factors.key(),"symbol_factors":self.factors.table(),"max_pos_u":self.config.max_pos_u,"allocation":"per_symbol_factor","strategies":strategies,"updated_ts_us":now,"scheduled_entries":0});
        atomic_json(Path::new(CTA_SPREAD_STATUS_PATH), &status)?;
        atomic_json(Path::new(SHADOW_PATH), &self.positions)
    }
}
/// Compute one absolute target transition; no exchange calls or partial updates.
fn advance_position(
    position: &mut StrategyPosition,
    bar: Option<&WeightedBar>,
    config: &CtaSpreadConfig,
    model_ts_ms: i64,
    live_after_ms: i64,
    now_us: i64,
    cash_quote: Option<Quote>,
) -> Result<&'static str> {
    position.validate()?;
    if !config.enabled {
        return Ok("publishing_disabled");
    }
    if model_ts_ms <= live_after_ms {
        return Ok("historical_bar");
    }
    let Some(bar) = bar else {
        return Ok("factor_unready");
    };
    let next_flag = next_position_flag(position.flag, bar, config);
    if next_flag == position.flag {
        return Ok(if position.flag == 0 && bar.value != 0.0 {
            "spread_entry_blocked"
        } else {
            "hold"
        });
    }
    if next_flag == 0 {
        position.exit();
        return Ok("factor_exit");
    }
    let previous_flag = position.flag;
    let quote = cash_quote.filter(|quote| {
        quote.bid.is_finite()
            && quote.ask.is_finite()
            && quote.bid > 0.0
            && quote.ask >= quote.bid
            && quote.ts > 0
            && quote.ts <= now_us
            && now_us - quote.ts <= CTA_SPREAD_MAX_QUOTE_AGE_MS as i64 * 1000
    });
    let Some(quote) = quote else {
        if previous_flag != 0 {
            position.exit();
            return Ok("reversal_entry_unavailable");
        }
        return Ok("entry_quote_unavailable");
    };
    let mut next = position.clone();
    next.exit();
    if let Err(error) = next.enter_cash(
        next_flag,
        config.max_pos_u,
        if next_flag > 0 { quote.bid } else { quote.ask },
    ) {
        if previous_flag != 0 {
            position.exit();
            return Ok("reversal_entry_unavailable");
        }
        return Err(error.context("size cash entry"));
    }
    *position = next;
    Ok(if previous_flag == 0 {
        "full_position_entry"
    } else {
        "factor_reversal"
    })
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let mut file = fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))?;
    fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(())
}
fn signal_snapshot(bar: &WeightedBar, flag: i8) -> Value {
    json!({"model_ts_ms":bar.model_ts_ms,"score":bar.value,"quantile":bar.quantile,"score_ready":true,"execution_coefficient":1.0,
        "factor_votes":bar.votes,"position_flag":flag,"decision":if flag > 0 { "long" } else if flag < 0 { "short" } else { "flat" },
        "spread_value":bar.spread.filter_long_value,"spread_quantile":bar.spread.filter_long_quantile,"filter_ready":bar.spread.filter_ready})
}

fn cash_symbol(symbol: &str) -> Result<&str> {
    let asset = symbol
        .strip_suffix("USDT")
        .context("CTA spread requires USDT cash/perpetual pairs")?;
    ensure!(!asset.is_empty(), "empty cash asset");
    Ok(asset)
}
fn market_targets(symbol: &str, p: &StrategyPosition) -> [(String, Value); 2] {
    // Factor scope and persisted state are validated before publication.
    [
        (
            symbol.strip_suffix("USDT").unwrap_or(symbol).to_string(),
            json!({"qty":p.cash_qty,"signal":0}),
        ),
        (
            symbol.to_string(),
            json!({"qty":p.perpetual_qty,"signal":1}),
        ),
    ]
}
const CASH_ORDER_STRATEGY: &str = "cta_spread_cash_maker";
const PERPETUAL_ORDER_STRATEGY: &str = "cta_spread_perpetual_taker";

fn position_payload(strategy_name: String, targets: &BTreeMap<String, Value>) -> Value {
    // Always supply both market policies. Omitted overrides would reset the
    // Manager mapping and allow the account binding's default algorithm to take over.
    let overrides: BTreeMap<_, _> = targets
        .keys()
        .map(|symbol| {
            (
                symbol.clone(),
                if symbol.ends_with("USDT") {
                    PERPETUAL_ORDER_STRATEGY
                } else {
                    CASH_ORDER_STRATEGY
                },
            )
        })
        .collect();
    json!({"strategy_name": strategy_name, "targets": targets,
        "symbol_order_strategy_overrides": overrides})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cta_spread::factors::{aggregate_bar, SymbolFactors};
    use crate::strategy::batch_exec_strategy::BatchExecConfig;
    use crate::strategy::pov::{ExecAlgorithm, PovLiquidity};
    use trade_signal::model_output_hub::ModelOutputScoreLookupResult;
    fn config() -> CtaSpreadConfig {
        serde_json::from_str(include_str!("../../config/cta_spread.json")).unwrap()
    }
    fn bar(factor: f64, spread: Option<f64>) -> WeightedBar {
        let input = ModelOutputScoreLookupResult {
            service_name: String::new(),
            symbol_key: "ETHUSDT".into(),
            subscribed: true,
            score: Some(1.0),
            score_quantile: Some(factor),
            score_ready: true,
            score_long_threshold: None,
            score_short_threshold: None,
            filter_long_value: Some(0.001),
            filter_long_threshold: None,
            filter_short_value: Some(0.001),
            filter_short_threshold: None,
            filter_long_quantile: spread,
            filter_short_quantile: spread,
            filter_ready: spread.is_some(),
            score_ts_ms: 60000,
            note: String::new(),
        };
        aggregate_bar(
            &SymbolFactors {
                factors: vec!["factor_036".into()],
                weights: vec![1.0],
            },
            &[input],
            &config().entry,
        )
        .unwrap()
    }

    const NOW: i64 = 60_000_000;
    fn quote() -> Quote {
        Quote {
            bid: 100.0,
            ask: 100.0,
            ts: NOW,
            ..Default::default()
        }
    }

    #[test]
    fn complete_signal_lifecycle_keeps_inventory_and_publishes_absolute_targets() {
        let mut c = config();
        c.enabled = true;
        let mut p = StrategyPosition::default();
        assert_eq!(
            advance_position(
                &mut p,
                Some(&bar(0.99, Some(0.2))),
                &c,
                60000,
                0,
                NOW,
                Some(quote())
            )
            .unwrap(),
            "full_position_entry"
        );
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (2.0, 0.0, 2.0));
        // Restart and a sizing change never resize an existing direction.
        p = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        c.max_pos_u = 300.0;
        assert_eq!(
            advance_position(
                &mut p,
                Some(&bar(0.99, Some(0.2))),
                &c,
                60000,
                0,
                NOW,
                Some(quote())
            )
            .unwrap(),
            "hold"
        );
        assert_eq!(p.qty, 2.0);
        assert_eq!(
            advance_position(
                &mut p,
                Some(&bar(0.01, Some(0.8))),
                &c,
                60000,
                0,
                NOW,
                Some(quote())
            )
            .unwrap(),
            "factor_reversal"
        );
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (-1.0, -2.0, -3.0));
        let targets = market_targets("ETHUSDT", &p);
        assert_eq!(
            advance_position(
                &mut p,
                Some(&bar(0.01, Some(0.8))),
                &c,
                60000,
                0,
                NOW,
                Some(quote())
            )
            .unwrap(),
            "hold"
        );
        assert_eq!(market_targets("ETHUSDT", &p), targets);
        // Exit works without any price or spread observations.
        assert_eq!(
            advance_position(&mut p, Some(&bar(0.8, None)), &c, 60000, 0, NOW, None).unwrap(),
            "factor_exit"
        );
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (-1.0, 1.0, 0.0));
    }

    #[test]
    fn disabled_historical_and_unready_observations_never_change_positions() {
        let mut c = config();
        let mut p = StrategyPosition::default();
        p.enter_cash(1, 200.0, 100.0).unwrap();
        let saved = p.clone();
        let reverse = bar(0.01, Some(0.8));
        assert_eq!(
            advance_position(&mut p, Some(&reverse), &c, 60000, 0, NOW, Some(quote())).unwrap(),
            "publishing_disabled"
        );
        assert_eq!(p, saved);
        c.enabled = true;
        assert_eq!(
            advance_position(&mut p, Some(&reverse), &c, 60000, 60000, NOW, Some(quote())).unwrap(),
            "historical_bar"
        );
        assert_eq!(p, saved);
        assert_eq!(
            advance_position(&mut p, None, &c, 60000, 0, NOW, Some(quote())).unwrap(),
            "factor_unready"
        );
        assert_eq!(p, saved);
    }

    #[test]
    fn invalid_cash_quote_blocks_entries_but_only_exits_reversals() {
        let mut c = config();
        c.enabled = true;
        for invalid in [
            None,
            Some(Quote {
                ts: NOW - 5_000_001,
                ..quote()
            }),
            Some(Quote {
                ts: NOW + 1,
                ..quote()
            }),
            Some(Quote {
                bid: f64::NAN,
                ..quote()
            }),
            Some(Quote {
                ask: 99.0,
                ..quote()
            }),
            Some(Quote {
                bid: 0.0,
                ..quote()
            }),
        ] {
            for (factor, spread, old_direction) in [(0.99, 0.2, -1), (0.01, 0.8, 1)] {
                let b = bar(factor, Some(spread));
                let mut p = StrategyPosition::default();
                assert_eq!(
                    advance_position(&mut p, Some(&b), &c, 60000, 0, NOW, invalid).unwrap(),
                    "entry_quote_unavailable"
                );
                assert_eq!(p, StrategyPosition::default());
                p.enter_cash(old_direction, 200.0, 100.0).unwrap();
                let old_cash = p.cash_qty;
                assert_eq!(
                    advance_position(&mut p, Some(&b), &c, 60000, 0, NOW, invalid).unwrap(),
                    "reversal_entry_unavailable"
                );
                assert_eq!(
                    (p.cash_qty, p.perpetual_qty, p.qty),
                    (old_cash, -old_cash, 0.0)
                );
            }
        }
    }

    #[test]
    fn unrepresentable_reverse_quantity_still_hedges_old_cash() {
        let mut c = config();
        c.enabled = true;
        let mut p = StrategyPosition::default();
        p.enter_cash(1, 200.0, 100.0).unwrap();
        let tiny = Quote {
            bid: 1e-308,
            ask: 1e-308,
            ..quote()
        };
        assert_eq!(
            advance_position(
                &mut p,
                Some(&bar(0.01, Some(0.8))),
                &c,
                60000,
                0,
                NOW,
                Some(tiny)
            )
            .unwrap(),
            "reversal_entry_unavailable"
        );
        assert_eq!((p.cash_qty, p.perpetual_qty, p.qty), (2.0, -2.0, 0.0));
    }

    #[test]
    fn entry_exit_and_reversal_publish_cash_maker_and_contract_taker() {
        for direction in [-1, 1] {
            let mut p = StrategyPosition::default();
            p.enter_cash(direction, 200.0, 100.0).unwrap();
            for stage in 0..3 {
                if stage == 1 {
                    p.exit();
                }
                if stage == 2 {
                    p.enter_cash(-direction, 300.0, 100.0).unwrap();
                }
                let payload = position_payload(
                    "factor_170".into(),
                    &market_targets("ETHUSDT", &p).into_iter().collect(),
                );
                assert_eq!(payload["targets"]["ETH"]["qty"], p.cash_qty);
                assert_eq!(payload["targets"]["ETHUSDT"]["qty"], p.perpetual_qty);
                assert_eq!(payload["targets"]["ETH"]["signal"], 0);
                assert_eq!(payload["targets"]["ETHUSDT"]["signal"], 1);
                assert_eq!(
                    payload["symbol_order_strategy_overrides"]["ETH"],
                    CASH_ORDER_STRATEGY
                );
                assert_eq!(
                    payload["symbol_order_strategy_overrides"]["ETHUSDT"],
                    PERPETUAL_ORDER_STRATEGY
                );
            }
        }
        assert!(cash_symbol("ETHUSDC").is_err());
    }

    #[test]
    fn prepared_manager_templates_enforce_market_liquidity_policies() {
        let templates: Vec<Value> = serde_json::from_str(include_str!(
            "../../config/cta_spread_order_strategies.json"
        ))
        .unwrap();
        let templates: BTreeMap<_, BatchExecConfig> = templates
            .into_iter()
            .map(|row| {
                (
                    row["strategy_name"].as_str().unwrap().to_string(),
                    serde_json::from_value(row["order_parameters"].clone()).unwrap(),
                )
            })
            .collect();
        let cash = &templates[CASH_ORDER_STRATEGY];
        cash.validate().unwrap();
        assert_eq!(cash.algorithm, ExecAlgorithm::Pov);
        assert_eq!(cash.pov.liquidity, PovLiquidity::MakerOnly);
        let perpetual = &templates[PERPETUAL_ORDER_STRATEGY];
        perpetual.validate().unwrap();
        assert_eq!(perpetual.algorithm, ExecAlgorithm::Ctaker);
    }
}
