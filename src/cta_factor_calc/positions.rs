//! Frequency position tables owned by the factor worker, fed directly from its
//! active replay generation. No IPC subscription or independent signal process.
use super::{
    config::{Combination, Config, Plan, Worker},
    engine::{Engine, FactorPoint},
    runtime::atomic_json,
};
use crate::cta_perp::{
    config::{CtaPerpConfig, PositionAlgorithm, SYMBOL_MEAN_STRATEGY},
    factor_channel::shared_input,
    factors::{aggregate_bar, mean_bar, SymbolFactors, WeightedBar},
    recovery::{Positions as PrepRows, StrategyPosition as PrepPosition},
};
use crate::cta_position_publisher::{add_target_quantity, PositionPublisher, Publication};
use crate::cta_spread::{config::SpreadMode, positions::StrategyPosition as SpreadPosition};
use anyhow::{ensure, Context, Result};
use order_common::TradingVenue;
use quote_plan::Quote;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use trade_signal::MktChannel;

type SpreadRows = BTreeMap<String, BTreeMap<String, SpreadPosition>>;

#[path = "migration.rs"]
pub mod migration;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "rows", rename_all = "snake_case")]
enum Ledger {
    Prep(PrepRows),
    Spread(SpreadRows),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    strategy_name: String,
    combination: Combination,
    ledger: Ledger,
    clock: BTreeMap<String, i64>,
    observations: BTreeMap<String, BTreeMap<String, Value>>,
    retiring: bool,
    retired: bool,
    ever_enabled: bool,
    updated_ts_us: i64,
    #[serde(skip)]
    live_after_ms: i64,
    #[serde(skip)]
    retirement_started_us: i64,
    #[serde(skip)]
    retirement_submitted: bool,
    #[serde(skip)]
    dirty: bool,
}

impl Table {
    fn new(combination: Combination, now_us: i64) -> Self {
        let spread = combination.reference_venue.is_some();
        Self {
            strategy_name: combination.manager_strategy_name(),
            combination,
            ledger: if spread {
                Ledger::Spread(BTreeMap::new())
            } else {
                Ledger::Prep(BTreeMap::new())
            },
            clock: BTreeMap::new(),
            observations: BTreeMap::new(),
            retiring: false,
            retired: false,
            ever_enabled: false,
            updated_ts_us: now_us,
            live_after_ms: now_us / 1000,
            retirement_started_us: 0,
            retirement_submitted: false,
            dirty: true,
        }
    }
    fn validate(&self) -> Result<()> {
        self.combination.validate()?;
        ensure!(
            self.strategy_name == self.combination.manager_strategy_name(),
            "position ledger identity changed; migrate Manager bindings explicitly"
        );
        match &self.ledger {
            Ledger::Prep(rows) => {
                ensure!(
                    self.combination.reference_venue.is_none(),
                    "Prep ledger has a Spread configuration"
                );
                for p in rows.values().flat_map(|r| r.values()) {
                    ensure!(
                        p.qty.is_finite()
                            && p.entry_notional.is_finite()
                            && p.entry_notional >= 0.0
                            && if p.flag == 0 {
                                p.qty == 0.0 && p.entry_notional == 0.0
                            } else {
                                matches!(p.flag, -1 | 1)
                                    && p.qty * f64::from(p.flag) > 0.0
                                    && p.entry_notional > 0.0
                            },
                        "invalid persisted Prep position"
                    );
                }
            }
            Ledger::Spread(rows) => {
                ensure!(
                    self.combination.reference_venue.is_some(),
                    "Spread ledger has a Prep configuration"
                );
                for p in rows.values().flat_map(|r| r.values()) {
                    p.validate()?;
                }
            }
        }
        self.targets()?;
        Ok(())
    }
    fn update(&mut self, next: &Combination, now_us: i64) -> Result<()> {
        if &self.combination == next && !self.retiring && !self.retired {
            return Ok(());
        }
        ensure!(
            self.strategy_name == next.manager_strategy_name(),
            "cannot rename a running position table"
        );
        if let Ledger::Prep(rows) = &self.ledger {
            if self.combination.signal_config()?.position_algorithm
                != next.signal_config()?.position_algorithm
            {
                ensure!(
                    rows.values().flat_map(|r| r.values()).all(|p| p.qty == 0.0),
                    "close factor sub-positions before changing the position algorithm"
                );
            }
        }
        self.combination = next.clone();
        self.live_after_ms = now_us / 1000;
        self.retiring = false;
        self.retired = false;
        self.retirement_submitted = false;
        self.dirty = true;
        self.sync_scope()?;
        Ok(())
    }
    fn sync_scope(&mut self) -> Result<()> {
        let config = self.combination.signal_config()?;
        let table = self.combination.signal_table()?;
        let mut scope = BTreeMap::<String, Vec<String>>::new();
        for (symbol, row) in &table {
            let names = if config.position_algorithm == PositionAlgorithm::V1 {
                vec![SYMBOL_MEAN_STRATEGY.into()]
            } else {
                config
                    .position_factors(symbol, row)
                    .into_iter()
                    .map(|r| r.strategy)
                    .collect()
            };
            scope.insert(symbol.clone(), names);
        }
        match &mut self.ledger {
            Ledger::Prep(rows) => {
                for (rule, positions) in rows {
                    for (symbol, p) in positions {
                        if self.retiring || !scope.get(symbol).is_some_and(|s| s.contains(rule)) {
                            *p = PrepPosition::default();
                        }
                    }
                }
            }
            Ledger::Spread(rows) => {
                for (rule, positions) in rows {
                    for (symbol, p) in positions {
                        if self.retiring || !scope.contains_key(symbol) {
                            *p = SpreadPosition::default();
                        } else if !scope[symbol].contains(rule) {
                            p.exit();
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn retire(&mut self, now_us: i64) -> Result<()> {
        if self.retiring || self.retired {
            return Ok(());
        }
        self.retiring = true;
        self.retirement_started_us = now_us;
        self.dirty = true;
        self.sync_scope()?;
        if !self.ever_enabled {
            self.retired = true;
        }
        Ok(())
    }
    fn targets(&self) -> Result<BTreeMap<String, Value>> {
        let mut totals = BTreeMap::new();
        let table = self.combination.signal_table()?;
        match &self.ledger {
            Ledger::Prep(rows) => {
                for positions in rows.values() {
                    for (symbol, p) in positions {
                        add_target_quantity(&mut totals, symbol, p.qty)?;
                    }
                }
                for symbol in table.keys() {
                    totals.entry(symbol.clone()).or_default();
                }
                Ok(totals
                    .into_iter()
                    .map(|(s, q)| {
                        (
                            s,
                            if q == 0.0 {
                                json!({"qty":"clear","signal":0})
                            } else {
                                json!({"qty":q,"signal":0})
                            },
                        )
                    })
                    .collect())
            }
            Ledger::Spread(rows) => {
                let mode = SpreadMode::OkexBinance;
                let mut clear = std::collections::BTreeSet::new();
                for positions in rows.values() {
                    for (symbol, p) in positions {
                        p.validate()?;
                        let (direction, hedge) = mode.target_symbols(symbol);
                        add_target_quantity(&mut totals, &direction, p.cash_qty)?;
                        add_target_quantity(&mut totals, &hedge, p.perpetual_qty)?;
                        if self.retiring || !table.contains_key(symbol) {
                            clear.extend([direction, hedge]);
                        }
                    }
                }
                for symbol in table.keys() {
                    let (a, b) = mode.target_symbols(symbol);
                    totals.entry(a.clone()).or_default();
                    totals.entry(b.clone()).or_default();
                    if self.retiring {
                        clear.extend([a, b]);
                    }
                }
                Ok(totals
                    .into_iter()
                    .map(|(s, q)| {
                        let value = if clear.contains(&s) {
                            json!({"qty":"clear","signal":1})
                        } else {
                            json!({"qty":q,"signal":0})
                        };
                        (s, value)
                    })
                    .collect())
            }
        }
    }
    fn enabled(&self) -> bool {
        self.combination.signal.as_ref().is_some_and(|s| s.enabled)
    }
    fn compatible(&self, active: &Plan) -> Result<bool> {
        for id in self.combination.factors()?.keys() {
            let desired = self.combination.parameters.get(id).cloned().unwrap_or(
                super::config::Parameters::for_id(&super::config::FactorId::parse(id)?),
            );
            if !active
                .calculations
                .get(id)
                .is_some_and(|c| c.parameters == desired)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn observe(
        &mut self,
        points: &BTreeMap<String, FactorPoint>,
        previous: &BTreeMap<String, FactorPoint>,
        now_us: i64,
        quote: &impl Fn(&str, TradingVenue) -> Option<Quote>,
    ) -> Result<()> {
        if self.retiring || self.retired || !self.enabled() {
            return Ok(());
        }
        self.ever_enabled = true;
        let config = self.combination.signal_config()?;
        for (symbol, row) in self.combination.signal_table()? {
            if config.position_algorithm == PositionAlgorithm::V1 {
                let effective = config.for_symbol(&symbol);
                let mut factor_input = effective.clone();
                factor_input.entry.nq_change_enabled = false;
                let inputs = row
                    .factors
                    .iter()
                    .map(|f| shared_input(points, &symbol, f, &factor_input, now_us / 1000))
                    .collect::<Option<Vec<_>>>();
                if let Some(inputs) = inputs {
                    if let Some(first) = inputs.first() {
                        if inputs.iter().all(|i| i.score_ts_ms == first.score_ts_ms) {
                            self.observe_prep(
                                &symbol,
                                SYMBOL_MEAN_STRATEGY,
                                first.score_ts_ms,
                                mean_bar(&row, &inputs, &effective.entry),
                                &effective,
                                now_us,
                                quote,
                            )?;
                        }
                    }
                }
                continue;
            }
            for binding in config.position_factors(&symbol, &row) {
                let mut effective = config.for_strategy(&symbol, &binding.strategy);
                let spread = self.combination.reference_venue.is_some();
                // Read the factor independently so an absent NQ cannot gate
                // its exit. The original effective config still gates entries.
                let mut factor_input = effective.clone();
                factor_input.entry.nq_change_enabled = false;
                let Some(input) = shared_input(
                    points,
                    &symbol,
                    &binding.factor,
                    &factor_input,
                    now_us / 1000,
                ) else {
                    continue;
                };
                let ts = input.score_ts_ms;
                let key = format!("{}:{symbol}", binding.strategy);
                if ts <= self.live_after_ms
                    || ts > now_us / 1000
                    || now_us / 1000 - ts > config.bar_interval.max_model_age_ms() as i64
                    || self.clock.get(&key).is_some_and(|old| *old >= ts)
                {
                    continue;
                }
                if spread {
                    effective = config.for_strategy(&symbol, &binding.strategy);
                    let interval = self.combination.interval;
                    let spread_id = format!(
                        "okex-futures-binance-futures-{}-{}-spread",
                        symbol.to_ascii_lowercase(),
                        interval.name()
                    );
                    let lag = points
                        .get(&spread_id)
                        .filter(|p| p.ts_ms == ts - interval.millis())
                        .or_else(|| {
                            previous
                                .get(&spread_id)
                                .filter(|p| p.ts_ms == ts - interval.millis())
                        });
                    let nq_id = format!(
                        "binance-futures-{}-{}-nq",
                        symbol.to_ascii_lowercase(),
                        interval.name()
                    );
                    let nq_present = points.get(&nq_id).is_some_and(|p| p.ts_ms == ts);
                    // Allow independently calculated dependencies to arrive for
                    // this bar before committing its one decision opportunity.
                    if (lag.is_none() || (effective.entry.nq_change_enabled && !nq_present))
                        && now_us / 1000 - ts < 5_000
                    {
                        continue;
                    }
                    let mut spread_config = self.combination.spread_config()?;
                    spread_config.nq_change_enabled = effective.entry.nq_change_enabled;
                    spread_config.entry.factor_long_quantile = effective.entry.factor_long_quantile;
                    spread_config.entry.factor_short_quantile =
                        effective.entry.factor_short_quantile;
                    spread_config.entry.nq_long_quantile = effective.entry.nq_long_quantile;
                    spread_config.entry.nq_short_quantile = effective.entry.nq_short_quantile;
                    spread_config.execution.factor_exit_quantile_long =
                        effective.execution.factor_exit_quantile_long;
                    spread_config.execution.factor_exit_quantile_short =
                        effective.execution.factor_exit_quantile_short;
                    let mut bar = crate::cta_spread::factors::aggregate_bar(
                        &crate::cta_spread::factors::SymbolFactors {
                            factors: vec![binding.factor.clone()],
                            weights: vec![binding.weight.signum()],
                        },
                        &[input.clone()],
                        &spread_config.entry,
                    );
                    if let Some(bar) = &mut bar {
                        let field = lag.and_then(|p| p.fields.get("value"));
                        bar.spread.filter_ready = field.is_some_and(|f| f.ready);
                        bar.spread.filter_long_quantile = field.and_then(|f| f.quantile);
                        bar.spread.filter_short_quantile = field.and_then(|f| f.quantile);
                        bar.nq = nq_present.then_some(input);
                    }
                    let Ledger::Spread(rows) = &mut self.ledger else {
                        anyhow::bail!("wrong Spread ledger")
                    };
                    let p = rows
                        .entry(binding.strategy.clone())
                        .or_default()
                        .entry(symbol.clone())
                        .or_default();
                    let reason = crate::cta_spread::signal::advance_position(
                        p,
                        bar.as_ref(),
                        &spread_config,
                        ts,
                        self.live_after_ms,
                        now_us,
                        quote(&symbol, TradingVenue::OkexFutures),
                    )?;
                    let value = json!({"position":p,"model_ts_ms":ts,"reason":reason,"factor":binding.factor,"nq":bar.as_ref().and_then(|b|b.nq.as_ref()).map(|n|n.filter_long_quantile),"spread_quantile":bar.as_ref().map(|b|b.spread.filter_long_quantile)});
                    self.observations
                        .entry(binding.strategy)
                        .or_default()
                        .insert(symbol.clone(), value);
                    self.clock.insert(key, ts);
                    self.dirty = true;
                } else {
                    let bar = aggregate_bar(
                        &SymbolFactors {
                            factors: vec![binding.factor],
                            weights: vec![binding.weight.signum()],
                        },
                        &[input],
                        &effective.entry,
                        effective.position_algorithm,
                    );
                    self.observe_prep(
                        &symbol,
                        &binding.strategy,
                        ts,
                        bar,
                        &effective,
                        now_us,
                        quote,
                    )?;
                }
            }
        }
        Ok(())
    }
    fn observe_prep(
        &mut self,
        symbol: &str,
        rule: &str,
        ts: i64,
        bar: Option<WeightedBar>,
        config: &CtaPerpConfig,
        now_us: i64,
        quote: &impl Fn(&str, TradingVenue) -> Option<Quote>,
    ) -> Result<()> {
        let key = format!("{rule}:{symbol}");
        if ts <= self.live_after_ms
            || ts > now_us / 1000
            || now_us / 1000 - ts > config.bar_interval.max_model_age_ms() as i64
            || self.clock.get(&key).is_some_and(|old| *old >= ts)
        {
            return Ok(());
        }
        let nq_pending =
            config.entry.nq_change_enabled && bar.as_ref().is_some_and(|b| !b.nq.filter_ready);
        if nq_pending
            && self
                .observations
                .get(rule)
                .and_then(|rows| rows.get(symbol))
                .is_some_and(|old| {
                    old["model_ts_ms"].as_i64() == Some(ts) && old["reason"] == "nq_pending"
                })
        {
            return Ok(());
        }
        let Ledger::Prep(rows) = &mut self.ledger else {
            anyhow::bail!("wrong Prep ledger")
        };
        let p = rows
            .entry(rule.to_string())
            .or_default()
            .entry(symbol.to_string())
            .or_default();
        let mut reason = "factor_unready";
        let mut retry_nq = false;
        if let Some(bar) = &bar {
            let before = p.flag;
            let q = quote(symbol, TradingVenue::BinanceFutures)
                .filter(|q| q.ts > 0 && q.ts <= now_us && now_us - q.ts <= 5_000_000);
            let result = crate::cta_perp::recovery::advance_position(
                p,
                bar,
                config,
                q.map_or(f64::NAN, |q| q.bid),
                q.map_or(f64::NAN, |q| q.ask),
            );
            reason = if result.is_err() {
                "entry_quote_unavailable"
            } else if p.flag == before {
                "hold"
            } else if p.flag == 0 {
                "factor_exit"
            } else {
                "full_position_entry"
            };
            retry_nq = nq_pending
                && p.flag == 0
                && (before == 0
                    || config.bar_interval == crate::cta_perp::interval::BarInterval::OneHour);
            if retry_nq && p.flag == before {
                reason = "nq_pending";
            }
        }
        self.observations.entry(rule.to_string()).or_default().insert(symbol.to_string(),json!({"position":p,"model_ts_ms":ts,"reason":reason,"factor_quantile":bar.as_ref().map(|b|b.decision_value)}));
        if !retry_nq {
            self.clock.insert(key, ts);
        }
        self.dirty = true;
        Ok(())
    }
    fn save(&mut self, root: &Path, now_us: i64) -> Result<()> {
        if self.dirty {
            self.updated_ts_us = now_us;
            self.validate()?;
            atomic_json(&root.join(format!("{}.json", self.strategy_name)), self)?;
            let mut journal = OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join(format!("{}.events.jsonl", self.strategy_name)))?;
            serde_json::to_writer(
                &mut journal,
                &json!({"strategy_name":self.strategy_name,"updated_ts_us":now_us,"sub_positions":self.ledger,"observations":self.observations}),
            )?;
            journal.write_all(b"\n")?;
            journal.sync_data()?;
            self.dirty = false;
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<Value> {
        Ok(
            json!({"strategy_name":self.strategy_name,"strategy_group":self.combination.strategy_group,"symbol_group":self.combination.symbol_group,"combination_name":self.combination.name,"interval":self.combination.interval,
            "enabled":self.enabled() && !self.retiring && !self.retired,"max_pos_u":self.combination.signal.as_ref().map(|s|s.max_pos_u),
            "retiring":self.retiring,"retired":self.retired,"updated_ts_us":self.updated_ts_us,"targets":self.targets()?,"sub_positions":self.ledger,"observations":self.observations}),
        )
    }
}

pub(crate) struct PositionTables {
    root: PathBuf,
    tables: BTreeMap<String, Table>,
    publisher: PositionPublisher,
    worker: Worker,
    quotes_initialized: bool,
}
impl PositionTables {
    pub fn new(config: &Config, worker: Worker) -> Result<Self> {
        let root = config.history_dir.join("positions");
        fs::create_dir_all(&root)?;
        let mut tables = BTreeMap::new();
        let now_us = super::runtime::now_ms() * 1000;
        for entry in fs::read_dir(&root)? {
            let path = entry?.path();
            if path.extension().is_none_or(|s| s != "json") {
                continue;
            }
            let mut table: Table = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("restore position table {}", path.display()))?;
            table.validate()?;
            ensure!(
                path.file_stem().and_then(|s| s.to_str()) == Some(&table.strategy_name),
                "position filename/identity mismatch"
            );
            table.live_after_ms = now_us / 1000;
            table.retirement_started_us = now_us;
            tables.insert(table.strategy_name.clone(), table);
        }
        Ok(Self {
            root,
            tables,
            publisher: PositionPublisher::from_env()?,
            worker,
            quotes_initialized: false,
        })
    }
    pub fn pause(&self) {
        self.publisher.pause();
    }
    pub fn activate_generation(&mut self, now_us: i64) {
        for table in self.tables.values_mut() {
            table.live_after_ms = now_us / 1000;
        }
    }
    pub fn tick(&mut self, desired: &Plan, active: Option<&Engine>, now_us: i64) -> Result<()> {
        let wanted = desired
            .combinations
            .values()
            .filter(|c| c.signal.is_some())
            .map(|c| (c.manager_strategy_name(), c))
            .collect::<BTreeMap<_, _>>();
        for (name, combination) in &wanted {
            if let Some(table) = self.tables.get_mut(name) {
                table.update(combination, now_us)?;
            } else {
                self.tables
                    .insert(name.clone(), Table::new((*combination).clone(), now_us));
            }
        }
        for (name, table) in &mut self.tables {
            if !wanted.contains_key(name) {
                table.retire(now_us)?;
            }
        }
        if self
            .tables
            .values()
            .any(|t| t.enabled() && !t.retired && !t.retiring)
        {
            ensure!(!self.publisher.is_empty(),"CTA_MANAGER_API_URLS (or CTA_MANAGER_API_URL) is required for enabled position tables");
            if !self.quotes_initialized {
                let direction = if self.worker == Worker::Perp {
                    TradingVenue::BinanceFutures
                } else {
                    TradingVenue::OkexFutures
                };
                MktChannel::init_bbo_singleton_readonly(direction, TradingVenue::BinanceFutures)
                    .context("initialize position sizing BBO")?;
                self.quotes_initialized = true;
            }
        }
        let endpoints = self.publisher.statuses();
        for table in self.tables.values_mut() {
            if table.retiring
                && table.retirement_submitted
                && !table.retired
                && !endpoints.is_empty()
                && endpoints.iter().all(|e| {
                    !e.pending_strategies.contains(&table.strategy_name)
                        && e.error.is_none()
                        && e.last_success_ts_us
                            .is_some_and(|ts| ts >= table.retirement_started_us)
                })
            {
                table.retired = true;
                table.dirty = true;
            }
            if let Some(engine) = active {
                if table.compatible(&engine.plan)? {
                    table.observe(&engine.latest, &engine.previous_spreads, now_us, &|s, v| {
                        MktChannel::instance().get_quote(s, v)
                    })?;
                }
            }
            table.save(&self.root, now_us)?;
        }
        // Delivery receipts are independent of factor polling and contain no credentials.
        for delivery in self.publisher.drain_deliveries() {
            let Some(name) = delivery.publication.payload["strategy_name"].as_str() else {
                continue;
            };
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.root.join(format!("{name}.deliveries.jsonl")))?;
            serde_json::to_writer(
                &mut file,
                &json!({"manager_url":delivery.manager_url,"published_ts_us":delivery.published_ts_us,"target":delivery.publication.payload}),
            )?;
            file.write_all(b"\n")?;
        }
        let mut publications = BTreeMap::new();
        for (name, table) in &self.tables {
            if !table.retired
                && (table.retiring
                    || (table.enabled()
                        && active.is_some_and(|a| table.compatible(&a.plan).unwrap_or(false))))
            {
                publications.insert(
                    name.clone(),
                    Publication {
                        payload: json!({"strategy_name":name,"targets":table.targets()?}),
                        context: json!({}),
                    },
                );
            }
        }
        self.publisher.submit(publications);
        for table in self.tables.values_mut() {
            if table.retiring && !table.retired {
                table.retirement_submitted = true;
            }
        }
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Value> {
        Ok(
            json!({"tables":self.tables.values().map(Table::snapshot).collect::<Result<Vec<_>>>()?,"manager_publications":self.publisher.statuses(),"publication_error":self.publisher.error()}),
        )
    }
}

#[cfg(test)]
#[path = "positions_tests.rs"]
mod tests;
